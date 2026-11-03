//! HTTP end-to-end soak tests for the real `chronostrace` proxy binary.
//!
//! Unlike the in-process dispatch tests in `chronostrace-proxy`, these
//! tests spawn the actual CLI binary in record mode, drive it over
//! real HTTP through `reqwest`, and inspect the cassette it persists
//! on graceful shutdown. The mock upstream is deterministic: it
//! selects its behavior from a `soak:<behavior>` marker in the
//! request body (the proxy forwards the body verbatim to
//! `{upstream}{proxy-path}`, so behavior cannot be selected by path).
//!
//! Timeouts are 2s throughout; the "long" scenarios are scaled so
//! the test proves the property in reasonable time, and every
//! latency claim below is a range over repeated runs, never a
//! single measurement.

use axum::response::sse::{Event, Sse};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use chronostrace_core::Cassette;
use futures_util::stream;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TIMEOUT_SECS: u64 = 2;
// Serializes the two heavy tests (cap, stalled-resource) within
// one test binary. Each holds ~2x its session count in fds
// inside the TEST process (client sockets + mock-server sockets),
// so two heavy tests at once can exceed the default 4096 fd
// limit and fail with EMFILE in stop()'s cassette read — a
// harness limit, not a proxy bug (the proxy itself holds ~2x
// cap ≈ 2200 fds at full cap, under the same limit). Serializing
// keeps each binary's peak around one heavy test's footprint.
static HEAVY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------- mock

#[derive(Default)]
struct MockStats {
    fast: AtomicUsize,
    slow_headers: AtomicUsize,
    never: AtomicUsize,
    slow_sse: AtomicUsize,
    stalled_sse: AtomicUsize,
    stall_forever: AtomicUsize,
}

async fn mock_upstream() -> (String, Arc<MockStats>) {
    let stats = Arc::new(MockStats::default());
    let app = Router::new().route(
        "/v1/chat/completions",
        post({
            let stats = stats.clone();
            move |Json(body): Json<Value>| {
                let stats = stats.clone();
                async move {
                    let marker = body["messages"][0]["content"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    match marker.as_str() {
                        "soak:slow-headers" => {
                            stats.slow_headers.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            Json(json!({"choices": [{"message": {"content": "slow ok"}}]}))
                                .into_response()
                        }
                        "soak:never" => {
                            stats.never.fetch_add(1, Ordering::SeqCst);
                            // Never responds. The proxy's timeout must
                            // cancel this; the handler is dropped with
                            // the connection, so nothing leaks.
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                            Json(json!({"choices": [{"message": {"content": "never"}}]}))
                                .into_response()
                        }
                        "soak:slow-sse" => {
                            stats.slow_sse.fetch_add(1, Ordering::SeqCst);
                            let s = stream::unfold(0u8, |step| async move {
                                if step < 12 {
                                    tokio::time::sleep(Duration::from_millis(400)).await;
                                    Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default()
                                                .data(format!("{{\"delta\":\"sse-{step}\"}}")),
                                        ),
                                        step + 1,
                                    ))
                                } else if step == 12 {
                                    Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("[DONE]"),
                                        ),
                                        13,
                                    ))
                                } else {
                                    None
                                }
                            });
                            Sse::new(s).into_response()
                        }
                        "soak:stalled-sse" => {
                            stats.stalled_sse.fetch_add(1, Ordering::SeqCst);
                            let s = stream::unfold(0u8, |step| async move {
                                match step {
                                    0 => Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("{\"delta\":\"stall-0\"}"),
                                        ),
                                        1,
                                    )),
                                    1 => Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("{\"delta\":\"stall-1\"}"),
                                        ),
                                        2,
                                    )),
                                    2 => {
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        Some((
                                            Ok::<_, std::convert::Infallible>(
                                                Event::default().data("{\"delta\":\"stall-2\"}"),
                                            ),
                                            3,
                                        ))
                                    }
                                    3 => Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("[DONE]"),
                                        ),
                                        4,
                                    )),
                                    _ => None,
                                }
                            });
                            Sse::new(s).into_response()
                        }
                        "soak:stall-forever" => {
                            stats.stall_forever.fetch_add(1, Ordering::SeqCst);
                            let s = stream::unfold(0u8, |step| async move {
                                if step == 0 {
                                    Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("{\"delta\":\"stalled\"}"),
                                        ),
                                        1,
                                    ))
                                } else {
                                    // Silence forever. The proxy must
                                    // not time this out (SSE policy),
                                    // but must notice a client drop
                                    // via tx.closed() in the pump.
                                    tokio::time::sleep(Duration::from_secs(3600)).await;
                                    None
                                }
                            });
                            Sse::new(s).into_response()
                        }
                        _ => {
                            stats.fast.fetch_add(1, Ordering::SeqCst);
                            Json(json!({"choices": [{"message": {"content": "fast ok"}}]}))
                                .into_response()
                        }
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), stats)
}

// ------------------------------------------------------------ proxy guard

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ct-soak-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A running proxy binary. `stop()` SIGTERMs it (graceful shutdown
/// persists the cassette) and returns the persisted node count.
/// `Drop` SIGTERMs + SIGKILLs as a last resort so a failed test
/// never leaks a proxy process.
struct Proxy {
    child: Option<Child>,
    base: String,
    cassette: PathBuf,
}

impl Proxy {
    fn pid(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }
    async fn start(tag: &str, mock_base: &str) -> Self {
        Self::start_with_timeout(tag, mock_base, TIMEOUT_SECS).await
    }

    /// Cap/concurrency/resource tests do NOT measure timeout
    /// behavior, so they run with a generous timeout: under
    /// extreme parallel load (several soak binaries at once),
    /// a 2s timeout fires on genuinely fast requests that are
    /// merely CPU-starved, which corrupts exact-count
    /// assertions (admitted/rejected) with 504s. Timeout
    /// behavior itself is covered by the dedicated 2s tests.
    async fn start_with_timeout(tag: &str, mock_base: &str, secs: u64) -> Self {
        // Up to 3 attempts: the freed port can theoretically be
        // reclaimed by another process between our probe bind and
        // the binary's bind. A binary that fails to bind exits
        // immediately, which we detect via try_wait (a readiness
        // connect alone could succeed against the wrong process).
        for _ in 0..3 {
            if let Some(proxy) = Self::try_start(tag, mock_base, secs).await {
                return proxy;
            }
        }
        panic!("proxy did not start after 3 attempts (port race?)");
    }

    async fn try_start_on(tag: &str, mock_base: &str, port: u16, secs: u64) -> Option<Self> {
        let dir = tmp_dir(tag);
        let cassette = dir.join("soak.ctrace");
        let bind = format!("127.0.0.1:{port}");
        let bin = env!("CARGO_BIN_EXE_chronostrace");
        let mut child = Command::new(bin)
            .args([
                "replay",
                "--cassette",
                cassette.to_str().unwrap(),
                "--mode",
                "record",
                "--upstream",
                mock_base,
                "--upstream-timeout-secs",
                &secs.to_string(),
                "--bind",
                &bind,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn proxy binary");
        // Readiness must PROVE this port speaks our proxy, not
        // merely accept TCP: a dummy listener (or another
        // process) can accept connects while our binary failed
        // to bind. POSTing an unknown path returns axum 404
        // without touching proxy state (no route → no dispatch
        // → no record), while a bare TCP acceptor never answers
        // HTTP. Combined with the child-alive check, a binary
        // that failed to bind is detected instead of mistaken
        // for ready. (This closes the most plausible mechanism
        // behind the one unreproduced parallel-workspace flake;
        // see docs/soak-test.md.)
        let probe = Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        let start = Instant::now();
        loop {
            if let Ok(Some(_)) = child.try_wait() {
                // Binary exited (e.g. bind conflict): retry.
                return None;
            }
            let resp = probe
                .post(format!("http://{bind}/__soak_ready__"))
                .json(&json!({}))
                .send()
                .await;
            if let Ok(resp) = resp {
                if resp.status() == StatusCode::NOT_FOUND {
                    // Re-check liveness: the child must still own
                    // this port (it could have exited mid-probe).
                    if child.try_wait().ok().flatten().is_some() {
                        return None;
                    }
                    break;
                }
            }
            if start.elapsed() > Duration::from_secs(10) {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Some(Proxy {
            child: Some(child),
            base: format!("http://{bind}"),
            cassette,
        })
    }

    async fn try_start(tag: &str, mock_base: &str, secs: u64) -> Option<Self> {
        Self::try_start_on(tag, mock_base, free_port(), secs).await
    }

    /// Graceful stop; returns the persisted cassette node count.
    /// Panics (after SIGKILL) if the proxy refuses to exit, so a
    /// wedged shutdown is a test failure, not a silent leak.
    fn stop(mut self) -> usize {
        let pid = self.child.as_ref().unwrap().id();
        let killed = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .unwrap();
        assert!(killed.success(), "SIGTERM to proxy {pid} failed");
        let start = Instant::now();
        let mut exited = false;
        while start.elapsed() < Duration::from_secs(10) {
            if let Ok(Some(_)) = self.child.as_mut().unwrap().try_wait() {
                exited = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if !exited {
            let _ = self.child.as_mut().unwrap().kill();
            let _ = self.child.as_mut().unwrap().wait();
            self.child.take();
            panic!("proxy {pid} did not shut down gracefully within 10s");
        }
        let _ = self.child.as_mut().unwrap().wait();
        self.child.take();
        Cassette::load_from_file(&self.cassette)
            .expect("persisted cassette must load")
            .nodes
            .len()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(5) {
                if let Ok(Some(_)) = child.try_wait() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// -------------------------------------------------------------- client

fn client() -> Client {
    Client::builder().no_proxy().build().unwrap()
}

fn soak_body(behavior: &str) -> Value {
    json!({"model": "gpt-4o", "messages": [{"role": "user", "content": format!("soak:{behavior}")}]})
}

async fn post_json(
    client: &Client,
    proxy: &str,
    session: Option<&str>,
    behavior: &str,
) -> (StatusCode, Value, Duration) {
    let mut req = client
        .post(format!("{proxy}/v1/chat/completions"))
        .json(&soak_body(behavior));
    if let Some(s) = session {
        req = req.header("x-chronostrace-session", s);
    }
    let start = Instant::now();
    let resp = req.send().await.expect("proxy request must succeed");
    let status = resp.status();
    let body: Value = resp.json().await.expect("proxy error body must be JSON");
    (status, body, start.elapsed())
}

/// POST an SSE request; returns status, (arrival_elapsed, data) per
/// event, and total duration. Arrival times are the proof for the
/// stalled-stream scenario: events after the stall must arrive long
/// past the 2s timeout, not error out at 2s.
async fn post_sse(
    client: &Client,
    proxy: &str,
    session: Option<&str>,
    behavior: &str,
) -> (StatusCode, Vec<(Duration, String)>, Duration) {
    let mut req = client
        .post(format!("{proxy}/v1/chat/completions"))
        .json(&soak_body(behavior));
    if let Some(s) = session {
        req = req.header("x-chronostrace-session", s);
    }
    let start = Instant::now();
    let mut resp = req.send().await.expect("proxy SSE request must succeed");
    let status = resp.status();
    let mut events = Vec::new();
    let mut buf = String::new();
    while let Some(chunk) = resp.chunk().await.expect("SSE chunk must arrive") {
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(idx) = buf.find("\n\n") {
            let block = buf[..idx].to_string();
            buf = buf[idx + 2..].to_string();
            for line in block.lines() {
                if let Some(data) = line.strip_prefix("data: ") {
                    events.push((start.elapsed(), data.trim().to_string()));
                }
            }
        }
    }
    (status, events, start.elapsed())
}

fn summarize(durs: &[Duration]) -> (Duration, Duration, Duration) {
    let min = *durs.iter().min().unwrap();
    let max = *durs.iter().max().unwrap();
    let sum: Duration = durs.iter().sum();
    (min, max, sum / (durs.len() as u32))
}

// ------------------------------------------------------------- scenarios

#[tokio::test]
async fn soak_long_sse_forwards_all_events_past_timeout() {
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start("long-sse", &mock).await;
    let client = client();
    let mut durs = Vec::new();
    for rep in 0..3 {
        let (status, events, total) = post_sse(
            &client,
            &proxy.base,
            Some(&format!("long-sse-{rep}")),
            "slow-sse",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "rep {rep}: SSE status");
        assert_eq!(events.len(), 13, "rep {rep}: 12 events + [DONE]");
        for (i, (_, data)) in events.iter().take(12).enumerate() {
            assert!(
                data.contains(&format!("sse-{i}")),
                "rep {rep}: event {i} in order"
            );
        }
        assert_eq!(events[12].1, "[DONE]", "rep {rep}: stream ends with [DONE]");
        // 12 x 400ms = 4.8s nominal; must exceed the 2s timeout.
        assert!(
            total >= Duration::from_secs(4),
            "rep {rep}: stream lasted {total:?}"
        );
        durs.push(total);
    }
    let (min, max, mean) = summarize(&durs);
    eprintln!("soak long-sse: 3 reps, durations min {min:?} max {max:?} mean {mean:?}, upstream slow-sse hits {}", stats.slow_sse.load(Ordering::SeqCst));
    let nodes = proxy.stop();
    assert_eq!(nodes, 3, "one node per completed SSE stream");
}

#[tokio::test]
async fn soak_slow_upstream_headers_time_out() {
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start("slow-headers", &mock).await;
    let client = client();
    let mut durs = Vec::new();
    for rep in 0..2 {
        let (status, body, elapsed) = post_json(
            &client,
            &proxy.base,
            Some(&format!("slow-{rep}")),
            "slow-headers",
        )
        .await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "rep {rep}");
        assert_eq!(body["error"], "CHRONOSTRACE_UPSTREAM_TIMEOUT", "rep {rep}");
        assert!(
            elapsed >= Duration::from_millis(1500),
            "rep {rep}: waited {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(4500),
            "rep {rep}: must not wait for 5s upstream, took {elapsed:?}"
        );
        durs.push(elapsed);
    }
    let (min, max, mean) = summarize(&durs);
    eprintln!("soak slow-headers: 2 reps, timeout latency min {min:?} max {max:?} mean {mean:?}, upstream hits {}", stats.slow_headers.load(Ordering::SeqCst));
    assert_eq!(stats.slow_headers.load(Ordering::SeqCst), 2, "no retry");
    let nodes = proxy.stop();
    assert_eq!(nodes, 0, "timeouts record no nodes");
}

#[tokio::test]
async fn soak_nonresponsive_upstream_times_out() {
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start("never", &mock).await;
    let client = client();
    let mut durs = Vec::new();
    for rep in 0..2 {
        let (status, body, elapsed) =
            post_json(&client, &proxy.base, Some(&format!("never-{rep}")), "never").await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "rep {rep}");
        assert_eq!(body["error"], "CHRONOSTRACE_UPSTREAM_TIMEOUT", "rep {rep}");
        assert!(
            elapsed >= Duration::from_millis(1500),
            "rep {rep}: waited {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(4500),
            "rep {rep}: took {elapsed:?}"
        );
        durs.push(elapsed);
    }
    let (min, max, mean) = summarize(&durs);
    eprintln!("soak never: 2 reps, timeout latency min {min:?} max {max:?} mean {mean:?}, upstream hits {}", stats.never.load(Ordering::SeqCst));
    assert_eq!(stats.never.load(Ordering::SeqCst), 2, "no retry");
    let nodes = proxy.stop();
    assert_eq!(nodes, 0, "timeouts record no nodes");
}

#[tokio::test]
async fn soak_sse_stalled_stream_survives_timeout_then_recovers() {
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start("stalled", &mock).await;
    let client = client();
    let (status, events, total) =
        post_sse(&client, &proxy.base, Some("stalled-0"), "stalled-sse").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(events.len(), 4, "2 pre-stall + 1 post-stall + [DONE]");
    assert!(
        events[0].0 < Duration::from_millis(1500),
        "first event fast: {:?}",
        events[0].0
    );
    assert!(
        events[1].0 < Duration::from_millis(1500),
        "second event fast: {:?}",
        events[1].0
    );
    // The post-stall event must arrive AFTER the 2s timeout would
    // have fired: the stream was not killed by a total timeout.
    assert!(
        events[2].0 >= Duration::from_secs(4),
        "post-stall event arrived at {:?}",
        events[2].0
    );
    assert!(
        events[2].0 < Duration::from_secs(10),
        "post-stall event arrived at {:?}",
        events[2].0
    );
    assert_eq!(events[3].1, "[DONE]");
    eprintln!(
        "soak stalled-sse: arrivals {:?}, total {total:?}",
        events.iter().map(|(d, _)| *d).collect::<Vec<_>>()
    );
    // Disconnect mid-stall, then the same session must work again:
    // no leaked session lock, no leaked connection.
    let req = client
        .post(format!("{}/v1/chat/completions", proxy.base))
        .json(&soak_body("stalled-sse"))
        .header("x-chronostrace-session", "stalled-drop");
    let mut resp = req.send().await.expect("stalled stream must start");
    assert_eq!(resp.status(), StatusCode::OK);
    let mut got = 0;
    while got < 2 {
        match resp.chunk().await {
            Ok(Some(_)) => got += 1,
            other => panic!("expected SSE chunks before drop, got {other:?}"),
        }
    }
    drop(resp);
    let (status, _, _) = tokio::time::timeout(
        Duration::from_secs(10),
        post_json(&client, &proxy.base, Some("stalled-drop"), "fast"),
    )
    .await
    .expect("request after dropped stream must not hang");
    assert_eq!(
        status,
        StatusCode::OK,
        "same session works after dropped stream"
    );
    eprintln!(
        "soak stalled-sse: upstream stalled hits {}",
        stats.stalled_sse.load(Ordering::SeqCst)
    );
    let nodes = proxy.stop();
    // Completed stalled stream (1) + recovery fast request (1).
    // The dropped stream records nothing.
    assert_eq!(nodes, 2, "dropped stream must not record a node");
}

#[tokio::test]
async fn soak_concurrent_sessions_do_not_block_each_other() {
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start_with_timeout("concurrent", &mock, 30).await;
    let client = client();
    let mut round_durs = Vec::new();
    for round in 0..2 {
        let start = Instant::now();
        let mut handles = Vec::new();
        for i in 0..50 {
            let client = client.clone();
            let base = proxy.base.clone();
            let session = format!("conc-{i}");
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                let (status, _, _) = post_json(&client, &base, Some(&session), "fast").await;
                (status, t0.elapsed())
            }));
        }
        let mut ok = 0;
        let mut lat = Vec::new();
        for h in handles {
            let (status, d) = h.await.unwrap();
            if status == StatusCode::OK {
                ok += 1;
            }
            lat.push(d);
        }
        assert_eq!(ok, 50, "round {round}: all sessions succeed");
        let (min, max, mean) = summarize(&lat);
        eprintln!("soak concurrent round {round}: 50 sessions, wall {:?}, latency min {min:?} max {max:?} mean {mean:?}", start.elapsed());
        round_durs.push(start.elapsed());
    }
    let (min, max, mean) = summarize(&round_durs);
    eprintln!("soak concurrent: 2 rounds, wall min {min:?} max {max:?} mean {mean:?}, upstream fast hits {}", stats.fast.load(Ordering::SeqCst));
    let nodes = proxy.stop();
    assert_eq!(nodes, 100, "50 sessions x 2 rounds = 100 nodes");
}

#[tokio::test]
async fn soak_session_cap_holds_over_http() {
    let _heavy = HEAVY.lock().await;
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start_with_timeout("cap", &mock, 30).await;
    let client = client();
    let start = Instant::now();
    let mut handles = Vec::new();
    for i in 0..1100 {
        let client = client.clone();
        let base = proxy.base.clone();
        let session = format!("cap-{i}");
        handles.push(tokio::spawn(async move {
            post_json(&client, &base, Some(&session), "fast").await.0
        }));
    }
    let mut ok = 0;
    let mut rejected = 0;
    for h in handles {
        match h.await.unwrap() {
            StatusCode::OK => ok += 1,
            StatusCode::TOO_MANY_REQUESTS => rejected += 1,
            other => panic!("unexpected status under HTTP cap pressure: {other}"),
        }
    }
    let wall = start.elapsed();
    eprintln!("soak cap: 1100 sessions in {wall:?}, admitted {ok}, rejected {rejected}, upstream fast hits {}", stats.fast.load(Ordering::SeqCst));
    assert_eq!(ok, 1024, "exactly MAX_SESSIONS admitted");
    assert_eq!(rejected, 76, "the rest rejected with 429");
    assert_eq!(
        stats.fast.load(Ordering::SeqCst),
        1024,
        "rejected sessions never reach upstream"
    );
    let nodes = proxy.stop();
    assert_eq!(nodes, 1024, "one node per admitted session");
}

#[tokio::test]
async fn soak_recovery_after_timeouts_and_failures() {
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start("recovery", &mock).await;
    let client = client();
    let (status, body, _) = post_json(&client, &proxy.base, Some("rec-0"), "slow-headers").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(body["error"], "CHRONOSTRACE_UPSTREAM_TIMEOUT");
    let (status, _, _) = post_json(&client, &proxy.base, Some("rec-0"), "fast").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "same session recovers after timeout"
    );
    let (status, _, _) = post_json(&client, &proxy.base, Some("rec-1"), "never").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    let (status, _, _) = post_json(&client, &proxy.base, Some("rec-2"), "fast").await;
    assert_eq!(status, StatusCode::OK, "new session recovers after failure");
    eprintln!(
        "soak recovery: fast {} slow-headers {} never {}",
        stats.fast.load(Ordering::SeqCst),
        stats.slow_headers.load(Ordering::SeqCst),
        stats.never.load(Ordering::SeqCst)
    );
    let nodes = proxy.stop();
    assert_eq!(nodes, 2, "only the two successful requests recorded");
}

// REGRESSION for the parallel-workspace flake mechanism: a port
// owned by a process that accepts TCP but never speaks HTTP must
// NOT be mistaken for a ready proxy (a bare TCP-connect readiness
// check would succeed against it). With HTTP-404 readiness plus
// the child-alive check, the bind failure is detected and the
// attempt returns None; a genuinely free port still starts.
#[tokio::test]
async fn soak_proxy_start_rejects_occupied_port() {
    let (mock, _stats) = mock_upstream().await;
    let dummy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = dummy.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            if dummy.accept().await.is_err() {
                break;
            }
        }
    });
    let attempt = Proxy::try_start_on("occupied", &mock, port, TIMEOUT_SECS).await;
    assert!(
        attempt.is_none(),
        "bind failure must be detected, not mistaken for ready"
    );
    let proxy = Proxy::try_start_on("freeport", &mock, free_port(), TIMEOUT_SECS)
        .await
        .expect("free port must start");
    let nodes = proxy.stop();
    assert_eq!(nodes, 0, "readiness probe must not record a node");
}

fn rss_kb(pid: u32) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        rest.trim().strip_suffix("kB")?.trim().parse().ok()
    })
}

// RESOURCE AUDIT for stalled SSE (policy unchanged: no total
// timeout after headers). 200 streams stall forever while the
// test measures proxy RSS, proves other sessions still work,
// proves the stalled streams occupy cap slots, then drops every
// stream and proves the pump notices the drop mid-stall (via
// tx.closed) instead of leaking task, connection, guard, and
// reservation. Without the closed() fix, the same-session
// recovery request below would hang (bounded wait fails).
#[tokio::test]
async fn soak_sse_stalled_resource_impact() {
    let _heavy = HEAVY.lock().await;
    let (mock, stats) = mock_upstream().await;
    let proxy = Proxy::start_with_timeout("stall-resource", &mock, 30).await;
    let client = client();
    let baseline = rss_kb(proxy.pid());
    let mut stalled = Vec::new();
    for i in 0..200 {
        let mut resp = client
            .post(format!("{}/v1/chat/completions", proxy.base))
            .json(&soak_body("stall-forever"))
            .header("x-chronostrace-session", format!("stall-res-{i}"))
            .send()
            .await
            .expect("stalled stream must start");
        assert_eq!(resp.status(), StatusCode::OK);
        // First event proves the stream is established.
        let first = resp.chunk().await.expect("first chunk").expect("chunk ok");
        assert!(String::from_utf8_lossy(&first).contains("stalled"));
        stalled.push(resp);
    }
    let rss_stalled = rss_kb(proxy.pid());
    eprintln!(
        "soak stall-resource: 200 stalled streams, proxy RSS baseline {baseline:?} kB -> stalled {rss_stalled:?} kB"
    );
    // Other sessions are unaffected while 200 streams stall.
    let (status, _, fresh_lat) = post_json(&client, &proxy.base, Some("stall-fresh"), "fast").await;
    assert_eq!(status, StatusCode::OK, "fresh session works during stall");
    eprintln!("soak stall-resource: fresh-session latency during stall: {fresh_lat:?}");
    // Cap occupancy: 1 fresh + 200 stalled already hold slots,
    // so of 830 further new sessions exactly 823 are admitted.
    let mut handles = Vec::new();
    for i in 0..830 {
        let client = client.clone();
        let base = proxy.base.clone();
        let session = format!("stall-cap-{i}");
        handles.push(tokio::spawn(async move {
            post_json(&client, &base, Some(&session), "fast").await.0
        }));
    }
    let mut admitted = 0;
    let mut rejected = 0;
    for h in handles {
        match h.await.unwrap() {
            StatusCode::OK => admitted += 1,
            StatusCode::TOO_MANY_REQUESTS => rejected += 1,
            other => panic!("unexpected status in cap probe: {other}"),
        }
    }
    assert_eq!(admitted, 823, "stalled streams hold 200 cap slots");
    assert_eq!(rejected, 7, "the rest are rejected");
    eprintln!("soak stall-resource: cap probe admitted {admitted}, rejected {rejected}");
    // Drop every stalled stream and let the pump notice.
    drop(stalled);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rss_after = rss_kb(proxy.pid());
    eprintln!("soak stall-resource: proxy RSS after drop+settle: {rss_after:?} kB");
    // The same session that stalled must work again: its guard
    // was released mid-stall by the closed() path, not held
    // until the (never-coming) upstream bytes.
    let (status, _, _) = tokio::time::timeout(
        Duration::from_secs(10),
        post_json(&client, &proxy.base, Some("stall-res-0"), "fast"),
    )
    .await
    .expect("recovery after dropped stall must not hang");
    assert_eq!(
        status,
        StatusCode::OK,
        "stalled session recovers after drop"
    );
    let (status, _, _) = post_json(&client, &proxy.base, Some("stall-new"), "fast").await;
    assert_eq!(status, StatusCode::OK, "new session works after drop");
    assert_eq!(stats.stall_forever.load(Ordering::SeqCst), 200);
    assert_eq!(
        stats.fast.load(Ordering::SeqCst),
        1 + 823 + 2,
        "only successful requests reach upstream"
    );
    let nodes = proxy.stop();
    // 1 fresh + 823 cap probe + 2 recovery = 826; stalled
    // streams record nothing.
    assert_eq!(nodes, 826, "dropped stalls record no nodes");
}
