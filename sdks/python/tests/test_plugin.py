import py_compile
from pathlib import Path


def test_plugin_compiles_cleanly():
    plugin_file = Path(__file__).resolve().parent.parent / "pytest_chronostrace" / "plugin.py"
    assert plugin_file.exists()
    py_compile.compile(str(plugin_file), doraise=True)