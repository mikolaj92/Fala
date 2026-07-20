"""SQLite adapter smoke: schema bootstrap via SqliteJournalPort."""

from std.os import remove
from fala.sqlite_journal_port import SqliteJournalPort


def main() raises:
    var path = "/tmp/fala-core-sqlite-schema.sqlite"
    try:
        remove(path)
    except e:
        pass
    var port = SqliteJournalPort.open(path)
    if not port.runtime_uri().startswith("sqlite://"):
        raise Error("runtime_uri must be sqlite://")
    port.close()
    # Reopen proves durable schema
    var again = SqliteJournalPort.open(path)
    again.close()
    try:
        remove(path)
    except e:
        pass
    print("sqlite schema smoke ok")
