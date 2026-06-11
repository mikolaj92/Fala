from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeSchemaMigration:
    version: int
    migration_id: str
    description: str

    @property
    def checksum(self) -> str:
        payload = {
            "version": self.version,
            "migration_id": self.migration_id,
            "description": self.description,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()


RUNTIME_SCHEMA_MIGRATIONS = (
    RuntimeSchemaMigration(
        version=1,
        migration_id="0001_runtime_control_plane",
        description=(
            "Initial runtime control-plane schema for runs, documents, processes, "
            "claims, outputs, streams, checkpoints, projections, workers, and audit."
        ),
    ),
    RuntimeSchemaMigration(
        version=2,
        migration_id="0002_document_relations",
        description="Add first-class document relation metadata to runtime documents.",
    ),
)

RUNTIME_SCHEMA_VERSION = RUNTIME_SCHEMA_MIGRATIONS[-1].version


def runtime_schema_migration_rows() -> list[dict[str, object]]:
    return [
        {
            "version": migration.version,
            "migration_id": migration.migration_id,
            "description": migration.description,
            "checksum": migration.checksum,
        }
        for migration in RUNTIME_SCHEMA_MIGRATIONS
    ]
