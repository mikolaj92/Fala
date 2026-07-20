"""open_journal / from_journal façade for Mojo core + adapters."""

from fala.memory_journal import InMemoryJournal
from fala.memory_runtime import MemoryRuntime
from fala.memory_driver import MemoryDriver
from fala.sqlite_journal_port import SqliteJournalPort
from fala.jsonl_journal import JsonlJournal


struct OpenedJournal(Movable):
    """Result of open_journal: kind + runtime_uri (+ optional path)."""

    var kind: String
    var runtime_uri: String
    var path: String

    def __init__(out self, kind: String, runtime_uri: String, path: String = ""):
        self.kind = kind
        self.runtime_uri = runtime_uri
        self.path = path


def open_journal_kind(kind: String, path: String = "") raises -> OpenedJournal:
    """Open a journal description by kind: memory | sqlite | jsonl."""
    if kind == "memory":
        var journal = InMemoryJournal("memory://local")
        return OpenedJournal("memory", journal.runtime_uri())
    if kind == "sqlite":
        if path == "":
            raise Error("sqlite journal requires path")
        var port = SqliteJournalPort.open(path)
        var uri = port.runtime_uri()
        port.close()
        return OpenedJournal("sqlite", uri, path)
    if kind == "jsonl":
        if path == "":
            raise Error("jsonl journal requires path")
        var j = JsonlJournal(path)
        return OpenedJournal("jsonl", j.runtime_uri(), path)
    raise Error("unsupported journal kind: " + kind)


def open_memory_runtime(stream_id: String = "memory://runtime") -> MemoryRuntime:
    return MemoryRuntime(stream_id)


def open_memory_driver(stream_id: String = "memory://driver") -> MemoryDriver:
    return MemoryDriver(stream_id)
