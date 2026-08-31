from __future__ import annotations

import json
import sqlite3
import threading

import pytest


def _run(db, run_id: str = "run") -> None:

    from fala.host import _ensure_durable_schema

    _ensure_durable_schema(db)
    now = "2026-01-01T00:00:00Z"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO runs (id,status,schema_version,metadata,created_at,updated_at) "
            "VALUES (?, 'active', 6, '{}', ?, ?)",
            (run_id, now, now),
        )


def _process(db, process_id: str):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT status,input_json,output_json,error_json,metadata,attempt,started_at,finished_at "
            "FROM processes WHERE id=?",
            (process_id,),
        ).fetchone()


def test_record_in_process_success_is_durable_and_returns_original(tmp_path) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    _run(db)
    result = {"value": [1, 2]}
    calls = 0

    def operation():
        nonlocal calls
        calls += 1
        return result

    returned = fala.record_in_process(
        db_path=db,
        run_id="run",
        process_id="execution-1",
        inputs={"checkpoint": 4},
        metadata={"source": "test"},
        operation=operation,
    )

    assert returned is result
    assert calls == 1
    row = _process(db, "execution-1")
    assert row is not None
    assert row[0] == "succeeded"
    assert json.loads(row[1]) == {"checkpoint": 4}
    assert json.loads(row[2]) == result
    assert json.loads(row[3]) == {}
    assert json.loads(row[4]) == {"source": "test"}
    assert row[5] == 1
    assert row[6] and row[7]


def test_record_in_process_failure_is_durable_and_reraises_same_exception(
    tmp_path,
) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    _run(db)
    error = LookupError("missing")

    def operation():
        raise error

    with pytest.raises(LookupError) as caught:
        fala.record_in_process(
            db_path=db, run_id="run", process_id="execution-2", operation=operation
        )
    assert caught.value is error
    row = _process(db, "execution-2")
    assert row[0] == "failed"
    assert json.loads(row[2]) == {}
    assert json.loads(row[3]) == {"message": "missing", "type": "LookupError"}


def test_record_in_process_non_json_result_fails_closed(tmp_path) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    _run(db)
    with pytest.raises(TypeError, match="operation result is not JSON-recordable"):
        fala.record_in_process(
            db_path=db,
            run_id="run",
            process_id="execution-3",
            operation=lambda: object(),
        )
    row = _process(db, "execution-3")
    assert row[0] == "failed"
    assert json.loads(row[2]) == {}
    assert json.loads(row[3])["type"] == "TypeError"


def test_record_in_process_invalid_diagnostics_do_not_invoke_callback(tmp_path) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    _run(db)
    called = False

    def operation():
        nonlocal called
        called = True

    with pytest.raises(TypeError, match="inputs is not JSON-recordable"):
        fala.record_in_process(
            db_path=db,
            run_id="run",
            process_id="execution-4",
            inputs={"bad": object()},
            operation=operation,
        )
    assert called is False
    assert _process(db, "execution-4") is None


def test_record_in_process_is_single_flight_per_database(tmp_path) -> None:
    import fala

    db = tmp_path / "journal.sqlite"
    _run(db)
    entered = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def held():
        entered.set()
        assert release.wait(5)
        return {"ok": True}

    def first():
        try:
            fala.record_in_process(
                db_path=db, run_id="run", process_id="first", operation=held
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=first)
    thread.start()
    assert entered.wait(5)
    try:
        with pytest.raises(RuntimeError, match="execution already active"):
            fala.record_in_process(
                db_path=db,
                run_id="run",
                process_id="second",
                operation=lambda: {"unexpected": True},
            )
    finally:
        release.set()
        thread.join(5)
    assert not errors
    assert _process(db, "first")[0] == "succeeded"
    assert _process(db, "second") is None


def test_record_in_process_does_not_write_sqlite_from_cpython() -> None:
    import inspect

    from fala import host

    source = inspect.getsource(host.record_in_process)
    assert "sqlite3" not in source
    assert "INSERT INTO processes" not in source
    assert "UPDATE processes" not in source
