"""open_journal façade smoke."""

from std.os import remove
from fala.open_journal import open_journal_kind, open_memory_runtime


def main() raises:
    var mem = open_journal_kind("memory")
    if mem.kind != "memory":
        raise Error("expected memory kind")
    if not mem.runtime_uri.startswith("memory://"):
        raise Error("memory uri")

    var path = "/tmp/fala-open-journal.sqlite"
    try:
        remove(path)
    except e:
        pass
    var sql = open_journal_kind("sqlite", path)
    if sql.kind != "sqlite":
        raise Error("expected sqlite kind")
    if not sql.runtime_uri.startswith("sqlite://"):
        raise Error("sqlite uri")
    try:
        remove(path)
    except e:
        pass

    var runtime = open_memory_runtime("memory://facade")
    _ = runtime.create_run("run_f")
    if "run_f" not in runtime.runs:
        raise Error("runtime create_run via façade")

    print("open_journal smoke ok")
