from pathlib import Path
import re

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


def test_native_cli_surface_is_the_single_public_dispatcher() -> None:
    cli = (FALA / "cli.mojo").read_text(encoding="utf-8")
    surface = (FALA / "native_cli_surface.mojo").read_text(encoding="utf-8")
    package = (FALA / "__init__.mojo").read_text(encoding="utf-8")

    assert "def dispatch_native_command(" in surface
    assert "def dispatch_native_command(" not in cli
    for source in (cli, surface, package):
        assert "dispatch_command" not in source
    assert "from fala.native_cli_surface import dispatch_native_command" in cli
    assert "output = dispatch_native_command(command)" in cli
    assert "from .native_cli_surface import cli_surface_help, dispatch_native_command" in package
    assert len(surface.splitlines()) <= 250


def test_native_cli_surface_only_routes_commands() -> None:
    surface = (FALA / "native_cli_surface.mojo").read_text(encoding="utf-8")

    assert re.findall(r"^\s*def (\w+)\(", surface, re.MULTILINE) == [
        "dispatch_native_command"
    ]
    imports = re.findall(r"^from ([\w.]+) import", surface, re.MULTILINE)
    assert imports
    assert all(module.startswith("fala.native_cli_") for module in imports)
    # Response encoding and storage orchestration belong to command owners,
    # including implementations formerly inlined inside dispatch branches.
    assert r'{\"' not in surface
    assert "initialize_database(status_path)" not in surface
    assert "initialize_database(doctor_path)" not in surface


def test_remaining_native_cli_commands_have_explicit_owners() -> None:
    owners = {
        "parse": ("_integer_option",),
        "inspect": (
            "_schema_model", "_schema_impulse", "_schema_status_json",
            "_migration_metadata", "_status", "_graph", "_explain", "_bridge_rows",
        ),
        "ops": ("_init", "initialize_database", "_vacuum", "_db_status", "_rehearse"),
        "lifecycle": (
            "_create", "_transition", "_impulse_create", "_process_schedule",
            "_process_transition", "_association_append", "_reaction_blob",
            "_reaction_record", "_homeostat_transition", "_homeostat_domain_values",
            "_homeostat_domain",
        ),
    }
    sources = {
        path.stem: path.read_text(encoding="utf-8")
        for path in FALA.glob("native_cli_*.mojo")
    }
    for owner, functions in owners.items():
        for function in functions:
            actual = [
                name for name, source in sources.items()
                if f"def {function}(" in source
            ]
            assert actual == [f"native_cli_{owner}"], function
    for owner in ("parse", "inspect", "ops", "lifecycle"):
        assert "native_cli_surface import" not in sources[f"native_cli_{owner}"]
