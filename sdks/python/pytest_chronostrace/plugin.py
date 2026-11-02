import socket
import subprocess
import time
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "chronostrace(cassette, chaos_rate_limit_prob=None): Deterministic DAG replay and chaos injection for LLM/MCP tests.",
    )


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(host: str, port: int, proc: subprocess.Popen, timeout: float = 4.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _, stderr = proc.communicate(timeout=1)
            raise RuntimeError(
                f"chronostrace exited early with code {proc.returncode}: {stderr.decode('utf-8', 'replace')}"
            )
        try:
            with socket.create_connection((host, port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError(f"Timed out waiting for chronostrace daemon on {host}:{port}")


@pytest.fixture(autouse=True)
def _chronostrace_harness(request, monkeypatch):
    marker = request.node.get_closest_marker("chronostrace")
    if marker is None:
        yield
        return

    cassette_path = marker.args[0] if marker.args else marker.kwargs.get("cassette")
    chaos_prob = marker.kwargs.get("chaos_rate_limit_prob")
    port = _find_free_port()
    bind_addr = f"127.0.0.1:{port}"

    cmd = ["chronostrace", "replay", "--cassette", str(cassette_path), "--bind", bind_addr]
    if chaos_prob is not None:
        cmd.extend(["--chaos-profile", "http-429", "--chaos-probability", str(chaos_prob)])

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_port("127.0.0.1", port, proc)
        monkeypatch.setenv("OPENAI_BASE_URL", f"http://{bind_addr}/v1")
        monkeypatch.setenv("ANTHROPIC_BASE_URL", f"http://{bind_addr}")
        monkeypatch.setenv("CHRONOSTRACE_MCP_PROXY", f"http://{bind_addr}/mcp/call")
        yield
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=1)