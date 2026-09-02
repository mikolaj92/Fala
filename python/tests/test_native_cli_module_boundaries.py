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
