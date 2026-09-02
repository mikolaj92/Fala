from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FALA = ROOT / "mojo" / "fala"


def test_native_cli_parser_has_its_own_module() -> None:
    parser = (FALA / "native_cli_parse.mojo").read_text(encoding="utf-8")
    surface = (FALA / "native_cli_surface.mojo").read_text(encoding="utf-8")

    for definition in ("def _word(", "def _flag(", "def _path(", "def _validate("):
        assert definition in parser
        assert definition not in surface
    assert "from fala.native_cli_parse import" in surface


def test_native_cli_inspect_has_its_own_module() -> None:
    inspect = (FALA / "native_cli_inspect.mojo").read_text(encoding="utf-8")
    surface = (FALA / "native_cli_surface.mojo").read_text(encoding="utf-8")

    for definition in ("def _runs(", "def _run_inspect(", "def _process_inspect(", "def _trace("):
        assert definition in inspect
        assert definition not in surface
    assert "from fala.native_cli_inspect import" in surface


def test_native_cli_ops_has_its_own_module() -> None:
    ops = (FALA / "native_cli_ops.mojo").read_text(encoding="utf-8")
    surface = (FALA / "native_cli_surface.mojo").read_text(encoding="utf-8")

    for definition in ("def _maintain_journal(", "def _gc(", "def _projection_rebuild(", "def _bridge_export(", "def _bridge_import(", "def _bridge_deliver("):
        assert definition in ops
        assert definition not in surface
    assert "from fala.native_cli_ops import" in surface
