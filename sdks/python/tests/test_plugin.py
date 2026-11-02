import py_compile
import socket
import subprocess
import sys
from pathlib import Path
import pytest

from pytest_chronostrace.plugin import _find_free_port, _wait_for_port


def test_plugin_compiles_cleanly():
    plugin_file = Path(__file__).resolve().parent.parent / "pytest_chronostrace" / "plugin.py"
    assert plugin_file.exists()
    py_compile.compile(str(plugin_file), doraise=True)


def test_find_free_port_returns_valid_ephemeral_port():
    port = _find_free_port()
    assert isinstance(port, int)
    assert 1024 <= port <= 65535


def test_wait_for_port_detects_early_process_exit():
    proc = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stderr.write('boom\\n'); sys.exit(2)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    with pytest.raises(RuntimeError, match="exited early with code 2"):
        _wait_for_port("127.0.0.1", 59999, proc, timeout=1.0)


def test_wait_for_port_succeeds_when_socket_is_listening():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]

        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(0.5)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            _wait_for_port("127.0.0.1", port, proc, timeout=1.0)
        finally:
            proc.terminate()
            proc.wait(timeout=1)