//! Lifecycle regression tests for `mcp-wrap` record mode (CT-HARDEN-006).
//!
//! Each test spawns the real `chronostrace` binary with a fake Python
//! child MCP server, drives it over stdio, and inspects the persisted
//! cassette. These are process-level tests: they assert on exit status,
//! on the frames the host actually receives, and on cassette contents.

use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;

fn python_cmd() -> &'static str {
    // Prefer python3; fall back to python where only that exists.
    if Command::new("python3").arg("--version").output().is_ok() {
        "python3"
    } else {
        "python"
    }
}

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ct-mcp-lifecycle-{}-{}", tag, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_child(dir: &Path, name: &str, script: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, script).unwrap();
    path
}

struct Harness {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
}

impl Harness {
    fn spawn(cassette: &PathBuf, child_script: &PathBuf) -> Harness {
        let bin = env!("CARGO_BIN_EXE_chronostrace");
        let mut child = Command::new(bin)
            .arg("mcp-wrap")
            .arg("-c")
            .arg(cassette)
            .arg("--mode")
            .arg("record")
            .arg("--")
            .arg(python_cmd())
            .arg(child_script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout: ChildStdout = child.stdout.take().unwrap();
        let (tx, rx) = channel::<String>();
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        Harness {
            child,
            stdin: Some(stdin),
            lines: rx,
        }
    }

    fn send(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().unwrap();
        stdin.write_all(line.as_bytes()).unwrap();
        stdin.write_all(b"\n").unwrap();
        stdin.flush().unwrap();
    }

    fn recv(&self) -> Value {
        let line = self
            .lines
            .recv_timeout(Duration::from_secs(15))
            .expect("timed out waiting for a host frame");
        serde_json::from_str(&line).unwrap()
    }

    fn close_stdin(&mut self) {
        // Dropping the owned handle closes the wrapper's stdin (HostEof).
        drop(self.stdin.take());
    }

    fn wait_exit(&mut self) -> std::process::ExitStatus {
        if let Some(stdin) = self.stdin.as_mut() {
            let _ = stdin.flush();
        }
        self.child
            .wait_timeout(Duration::from_secs(20))
            .expect("timed out waiting for wrapper exit")
            .expect("wrapper exited without a status")
    }
}

// std::process::Child::wait_timeout is unstable; emulate it.
trait WaitTimeout {
    fn wait_timeout(
        &mut self,
        timeout: Duration,
    ) -> std::io::Result<Option<std::process::ExitStatus>>;
}

impl WaitTimeout for Child {
    fn wait_timeout(
        &mut self,
        timeout: Duration,
    ) -> std::io::Result<Option<std::process::ExitStatus>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match self.try_wait()? {
                Some(status) => return Ok(Some(status)),
                None => {
                    if std::time::Instant::now() >= deadline {
                        return Ok(None);
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
}

fn read_cassette(path: &PathBuf) -> Value {
    let text = fs::read_to_string(path).unwrap();
    // Cassettes persist as YAML; parse via YAML, then normalize through
    // JSON so field access below stays uniform.
    let yaml: Value = serde_yaml::from_str(&text).unwrap();
    serde_json::from_str(&serde_json::to_string(&yaml).unwrap()).unwrap()
}

fn req(id: i64, method: &str, params: Value) -> String {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string()
}

#[test]
fn child_crash_mid_flight_yields_error_frame_and_nonzero_exit() {
    let dir = tmp_dir("crash");
    let cassette = dir.join("crash.ctrace");
    let script = write_child(
        &dir,
        "child.py",
        r#"
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    if req.get("id") == 1:
        print(json.dumps({"jsonrpc": "2.0", "id": 1, "result": "one"}), flush=True)
    elif req.get("id") == 2:
        sys.exit(3)
"#,
    );
    let mut h = Harness::spawn(&cassette, &script);
    h.send(&req(1, "tools/call", serde_json::json!({"name": "first"})));
    let resp1 = h.recv();
    assert_eq!(resp1["result"], "one");
    h.send(&req(2, "tools/call", serde_json::json!({"name": "second"})));
    // The host must get a JSON-RPC error frame for the dropped request,
    // not silence and not a fabricated success.
    let err_frame = h.recv();
    assert_eq!(err_frame["id"], 2);
    assert_eq!(err_frame["error"]["code"], -32603);
    let status = h.wait_exit();
    assert!(
        !status.success(),
        "wrapper must exit non-zero when the child dies mid-flight"
    );
    let ct = read_cassette(&cassette);
    let nodes = ct["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1, "only the answered request is recorded");
    assert_eq!(nodes[0]["response_payload"]["result"], "one");
}

#[test]
#[cfg(unix)]
fn signal_during_record_persists_partial_cassette_and_fails() {
    let dir = tmp_dir("signal");
    let cassette = dir.join("signal.ctrace");
    let script = write_child(
        &dir,
        "child.py",
        r#"
import sys, json, time
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    print(json.dumps({"jsonrpc": "2.0", "id": req.get("id"), "result": "ok"}), flush=True)
    while True:
        time.sleep(1)
"#,
    );
    let mut h = Harness::spawn(&cassette, &script);
    h.send(&req(1, "tools/call", serde_json::json!({"name": "first"})));
    let resp1 = h.recv();
    assert_eq!(resp1["result"], "ok");
    // SIGTERM the wrapper (not the child): the wrapper must persist the
    // partial cassette and exit non-zero instead of dying silently.
    let pid = h.child.id();
    let killed = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(killed.success());
    let status = h.wait_exit();
    assert!(!status.success(), "interrupted wrapper must exit non-zero");
    let ct = read_cassette(&cassette);
    let nodes = ct["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1, "partial cassette keeps the answered node");
    assert_eq!(nodes[0]["response_payload"]["result"], "ok");
}

#[test]
fn server_initiated_request_id_collision_is_not_recorded_as_response() {
    let dir = tmp_dir("collision");
    let cassette = dir.join("collision.ctrace");
    let script = write_child(
        &dir,
        "child.py",
        r#"
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    # Server-initiated request reusing the host request's id.
    print(json.dumps({"jsonrpc": "2.0", "id": 5, "method": "sampling/createMessage",
                      "params": {"messages": []}}), flush=True)
    print(json.dumps({"jsonrpc": "2.0", "id": 5, "result": "ok"}), flush=True)
"#,
    );
    let mut h = Harness::spawn(&cassette, &script);
    h.send(&req(5, "tools/call", serde_json::json!({"name": "first"})));
    let server_req = h.recv();
    assert_eq!(server_req["method"], "sampling/createMessage");
    let resp = h.recv();
    assert_eq!(resp["result"], "ok");
    h.close_stdin();
    let status = h.wait_exit();
    assert!(status.success());
    let ct = read_cassette(&cassette);
    let nodes = ct["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    // The recorded response is the real response, not the server request.
    assert_eq!(nodes[0]["response_payload"]["result"], "ok");
    assert!(nodes[0]["response_payload"].get("method").is_none());
}

#[test]
fn cancelled_request_does_not_block_later_records() {
    let dir = tmp_dir("cancel");
    let cassette = dir.join("cancel.ctrace");
    let script = write_child(
        &dir,
        "child.py",
        r#"
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    # The first request is never answered (it will be cancelled).
    if req.get("id") == 2:
        print(json.dumps({"jsonrpc": "2.0", "id": 2, "result": "second"}), flush=True)
"#,
    );
    let mut h = Harness::spawn(&cassette, &script);
    h.send(&req(1, "tools/call", serde_json::json!({"name": "slow"})));
    // Cancel the outstanding request, then send a second request.
    h.send(
        &serde_json::json!({"jsonrpc": "2.0", "method": "notifications/cancelled",
                           "params": {"requestId": 1}})
        .to_string(),
    );
    h.send(&req(2, "tools/call", serde_json::json!({"name": "fast"})));
    let resp = h.recv();
    assert_eq!(resp["result"], "second");
    h.close_stdin();
    let status = h.wait_exit();
    assert!(status.success());
    let ct = read_cassette(&cassette);
    let nodes = ct["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1, "only the answered request is recorded");
    assert_eq!(nodes[0]["response_payload"]["result"], "second");
    assert_eq!(nodes[0]["request_payload"]["params"]["name"], "fast");
}

#[test]
fn duplicate_host_request_ids_keep_first_tracking_only() {
    let dir = tmp_dir("dup");
    let cassette = dir.join("dup.ctrace");
    let script = write_child(
        &dir,
        "child.py",
        r#"
import sys, json
seen = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    seen.append(req)
    # Answer only after both requests arrived, so the duplicate is
    # guaranteed to be in flight when the wrapper reads it.
    if len(seen) == 2:
        for r in seen:
            name = r.get("params", {}).get("name", "")
            print(json.dumps({"jsonrpc": "2.0", "id": r.get("id"),
                              "result": name + "-ans"}), flush=True)
"#,
    );
    let mut h = Harness::spawn(&cassette, &script);
    // Send BOTH requests before reading any response: the second id-7
    // request must arrive while the first is still in flight. The child
    // answers only after seeing both, so this ordering is deterministic
    // rather than a race with the first response.
    h.send(&req(7, "tools/call", serde_json::json!({"name": "first"})));
    h.send(&req(7, "tools/call", serde_json::json!({"name": "second"})));
    let resp1 = h.recv();
    assert_eq!(resp1["result"], "first-ans");
    let resp2 = h.recv();
    assert_eq!(resp2["result"], "second-ans");
    h.close_stdin();
    let status = h.wait_exit();
    assert!(status.success());
    let ct = read_cassette(&cassette);
    let nodes = ct["nodes"].as_array().unwrap();
    assert_eq!(
        nodes.len(),
        1,
        "the duplicate id must not produce a second node"
    );
    assert_eq!(nodes[0]["request_payload"]["params"]["name"], "first");
}
