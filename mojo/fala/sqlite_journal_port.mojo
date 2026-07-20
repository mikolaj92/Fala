"""SqliteJournalPort — SQLite adapter over NativeJournal (not core identity).

Exposes runtime_uri and a thin façade. Full TX methods remain on NativeJournal;
callers that need create_run/claim use .engine directly until leading-unit
append_batch dispatch is complete.
"""

from fala.journal import NativeJournal


struct SqliteJournalPort(Movable):
    """Reference production sink implementing the JournalPort *surface*."""

    var engine: NativeJournal
    var path: String

    def __init__(out self, path: String) raises:
        self.path = path
        self.engine = NativeJournal(path)

    def runtime_uri(self) -> String:
        return "sqlite://" + self.path

    def initialize(mut self) raises:
        self.engine.initialize()

    def close(mut self) raises:
        self.engine.close()

    @staticmethod
    def open(path: String) raises -> SqliteJournalPort:
        var port = SqliteJournalPort(path)
        port.initialize()
        return port^
