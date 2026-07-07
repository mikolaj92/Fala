from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fala.artifacts import (
    FileArtifactStore,
    local_path_from_uri,
    resolve_artifact_local_path,
)
from fala.models import ArtifactRef


class ResolveArtifactLocalPathTests(unittest.TestCase):
    """One host-side resolver over both stored blobs and bare local URIs.

    A finished-run reader hands it whatever a step emitted -- an ``ArtifactRef``
    or the raw mapping -- and gets a local path or ``None``, without re-deriving
    the content-addressed blob layout or the artifact URI grammar itself.
    """

    def test_resolves_stored_blob_from_content_addressed_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "report.txt"
            source.write_bytes(b"artifact payload")
            store_root = root / "artifacts"
            ref = FileArtifactStore(store_root).put_file(kind="report", path=source)

            resolved = resolve_artifact_local_path(ref, store_root)
            self.assertIsNotNone(resolved)
            self.assertEqual(resolved.read_bytes(), b"artifact payload")
            # The raw mapping a step emitted resolves identically to the ref.
            self.assertEqual(
                resolve_artifact_local_path(ref.model_dump(mode="json"), store_root),
                resolved,
            )

    def test_falls_back_to_local_uri_when_not_a_stored_blob(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            local = root / "plain.txt"
            local.write_text("hi", encoding="utf-8")
            ref = ArtifactRef(id="a", kind="report", uri=local.as_uri())
            # Not a content-addressed blob -> the URI's own local path.
            self.assertEqual(
                resolve_artifact_local_path(ref, root / "artifacts"),
                local_path_from_uri(local.as_uri()),
            )

    def test_none_for_invalid_mapping_or_non_local_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store_root = Path(tmp_dir) / "artifacts"
            # A structurally invalid mapping is swallowed, not raised.
            self.assertIsNone(
                resolve_artifact_local_path({"nope": True}, store_root)
            )
            # A well-formed ref with a non-local scheme has no local path.
            remote = ArtifactRef(id="a", kind="report", uri="https://example.test/x")
            self.assertIsNone(resolve_artifact_local_path(remote, store_root))


if __name__ == "__main__":
    unittest.main()
