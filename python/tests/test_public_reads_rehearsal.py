from __future__ import annotations
import json
import os
import sqlite3
from pathlib import Path
import pytest


def _journal(path: Path):
    import fala
    fala.ensure_journal(path)
    fala.upsert_run_metadata(path, run_id="z", title="Z", status="active", metadata={"activation_id": "a"})
    fala.upsert_process(path, run_id="z", process_id="p", status="waiting", metadata={"effector_id": "e"}, inputs={"x": 1})
    fala.upsert_run_metadata(path, run_id="a", status="completed", metadata={})


def test_reads_are_sorted_json_safe_and_activation_metadata_is_generic(tmp_path):
    import fala
    db=tmp_path/'j.sqlite'; _journal(db)
    runs=fala.list_runs(db)
    assert [r['id'] for r in runs] == ['a','z']
    assert fala.get_run(db,'z')['metadata'] == {'activation_id':'a'}
    assert fala.get_run(db,'missing') is None
    process=fala.list_processes(db,'z')[0]
    assert process['metadata'] == {'effector_id':'e'} and process['input'] == {'x':1}
    json.dumps([runs, process], allow_nan=False)


def test_reads_fail_closed_on_caps_and_corrupt_json(tmp_path):
    import fala
    db=tmp_path/'j.sqlite'; _journal(db)
    with pytest.raises(RuntimeError, match='row limit'):
        fala.list_runs(db, max_rows=1)
    with sqlite3.connect(db) as c: c.execute("UPDATE runs SET metadata='{' WHERE id='z'")
    with pytest.raises(RuntimeError, match='malformed metadata'):
        fala.get_run(db,'z')


def test_rehearsal_captures_committed_wal_and_does_not_mutate_source(tmp_path, monkeypatch):
    import fala
    db=tmp_path/'live.sqlite'; _journal(db)
    live=sqlite3.connect(db); assert live.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
    live.execute("INSERT INTO runs (id,status,schema_version,metadata,created_at,updated_at) VALUES ('wal-only','active',6,'{}','now','now')"); live.commit()
    assert Path(str(db)+'-wal').stat().st_size
    before={p: p.read_bytes() for p in (db,Path(str(db)+'-wal'),Path(str(db)+'-shm'))}
    seen=[]
    original=fala.rehearsal.maintain_journal
    def maintain(snapshot, **kwargs):
        seen.append(snapshot)
        assert fala.get_run(snapshot,'wal-only')['id'] == 'wal-only'
        return original(snapshot, **kwargs)
    monkeypatch.setattr(fala.rehearsal, 'maintain_journal', maintain)
    out=fala.rehearse_journal_retention(db,tmp_path/'out',{'older_than_days':1,'keep_last':1,'safety_margin_bytes':0})
    assert len(seen)==1 and seen[0] != db
    assert not (tmp_path/'out'/'.incomplete').exists()
    assert (tmp_path/'out').stat().st_mode & 0o777 == 0o700
    assert db.read_bytes() == before[db]
    assert Path(str(db)+'-wal').read_bytes() == before[Path(str(db)+'-wal')]
    live.close()


def test_rehearsal_existing_destination_and_failure_marker(tmp_path, monkeypatch):
    import fala
    db=tmp_path/'j.sqlite'; _journal(db)
    existing=tmp_path/'existing'; existing.mkdir()
    with pytest.raises(FileExistsError): fala.rehearse_journal_retention(db,existing,{'older_than_days':1})
    monkeypatch.setattr(fala.rehearsal,'maintain_journal',lambda *a,**k: (_ for _ in ()).throw(RuntimeError('boom')))
    with pytest.raises(RuntimeError, match='boom'): fala.rehearse_journal_retention(db,tmp_path/'failed',{'older_than_days':1,'safety_margin_bytes':0})
    assert (tmp_path/'failed'/'.incomplete').exists()


def test_rehearsal_rejects_corrupt_source_and_symlink_destination(tmp_path):
    import fala
    bad=tmp_path/'bad.sqlite'; bad.write_bytes(b'not sqlite')
    with pytest.raises(sqlite3.DatabaseError): fala.rehearse_journal_retention(bad,tmp_path/'bad-out',{'older_than_days':1,'safety_margin_bytes':0})
    assert (tmp_path/'bad-out'/'.incomplete').exists()
    target=tmp_path/'target'; target.mkdir(); link=tmp_path/'link'; link.symlink_to(target)
    db=tmp_path/'j.sqlite'; _journal(db)
    with pytest.raises(FileExistsError): fala.rehearse_journal_retention(db,link,{'older_than_days':1})


def test_rehearsal_detects_source_path_swap(tmp_path, monkeypatch):
    import fala
    db=tmp_path/'j.sqlite'; replacement=tmp_path/'replacement.sqlite'
    _journal(db); _journal(replacement)
    original = fala.rehearsal.maintain_journal
    def swap(snapshot, **kwargs):
        plan = original(snapshot, **kwargs)
        os.replace(replacement, db)
        return plan
    monkeypatch.setattr(fala.rehearsal, 'maintain_journal', swap)
    with pytest.raises(RuntimeError, match='identity changed'):
        fala.rehearse_journal_retention(db,tmp_path/'swapped',{'older_than_days':1,'safety_margin_bytes':0})
    assert (tmp_path/'swapped'/'.incomplete').exists()


def test_rehearsal_rejects_committed_wal_mutation_of_snapshot(tmp_path, monkeypatch):
    import fala
    db = tmp_path / "j.sqlite"
    _journal(db)

    def mutate(snapshot, **kwargs):
        # A separate connection switches the snapshot back to WAL and commits a
        # durable mutation which can leave the main database bytes unchanged.
        writer = sqlite3.connect(snapshot)
        try:
            assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            writer.execute("UPDATE runs SET title='EVIL' WHERE id='z'")
            writer.commit()
        finally:
            writer.close()
        return {"dry_run": True}

    monkeypatch.setattr(fala.rehearsal, "maintain_journal", mutate)
    output = tmp_path / "mutated"
    with pytest.raises(RuntimeError, match="dry-run mutated"):
        fala.rehearse_journal_retention(
            db, output,
            {"older_than_days": 1, "keep_last": 1, "safety_margin_bytes": 0},
        )
    assert (output / ".incomplete").exists()
    assert not (output / "retention-plan.json").exists()
