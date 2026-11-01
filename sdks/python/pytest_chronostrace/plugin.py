import os
import subprocess
import time
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "chronostrace(cassette, chaos_rate_limit_prob=None): Deterministic DAG replay and chaos injection for LLM/MC
    )


@pytest.fixture(autouse=True)
def _chronostrace_harness(request, monkeypatch):
    marker = request.node.get_closest_marker("chronostrace")
    if marker is None:
        yield
        return

     cassette_path = marker.args[0] if marker.args else marker.kwargs.get("cassette")
     chaos_prob = marker.kwargs.get("chaos_rate_limit_prob")
     bind_addr = "127.0.0.1:18787"

     cmd = ["chronostrace", "replay", "--cassette", str(cassette_path), "--bind", bind_addr]
     if chaos_prob is not None:
         cmd.extend(["--chaos-rate-limit-prob", str(chaos_prob)])

     proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
     time.sleep(0.15) # Wait for local Tokio listener bind

     monkeypatch.setenv("OPENAI_BASE_URL", f"http://{bind_addr}/v1")
     monkeypatch.setenv("ANTHROPIC_BASE_URL", f"http://{bind_addr}")
     monkeypatch.setenv("CHRONOSTRACE_MCP_PROXY", f"http://{bind_addr}/mcp/call")

     try:
          yield





     finally:
         proc.terminate()
         proc.wait(timeout=3)
