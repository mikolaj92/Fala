from pathlib import Path
import tomllib


ROOT = Path(__file__).parents[2]

# Merge-gate Python files for the public host binding. Keep this list explicit
# in pixi.toml `python-host-api`; do not collapse it to `python/tests`.
PUBLIC_HOST_API_TESTS = (
    "python/tests/test_python_binding.py",
    "python/tests/test_in_process_recording.py",
    "python/tests/test_inspection.py",
    "python/tests/test_journal_lifecycle.py",
    "python/tests/test_public_reads_rehearsal.py",
    "python/tests/test_ensure_native_lock.py",
    "python/tests/test_wheel_contents.py",
    "python/tests/test_recovery.py",
    "python/tests/test_maintenance.py",
    "python/tests/test_mill_python_host_api.py",
)


def _task_cmd(tasks: dict, name: str) -> str:
    value = tasks[name]
    if isinstance(value, dict):
        return value["cmd"]
    return value


def test_full_smoke_lists_public_python_host_api():
    pixi = tomllib.loads((ROOT / "pixi.toml").read_text())
    tasks = pixi["tasks"]
    command = _task_cmd(tasks, "python-host-api")
    for path in PUBLIC_HOST_API_TESTS:
        assert path in command
    assert "python/tests " not in command + " "
    assert _task_cmd(tasks, "adapter-smoke").find("python-host-api") >= 0
    assert _task_cmd(tasks, "full-smoke").find("adapter-smoke") >= 0
