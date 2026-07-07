from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from io import BytesIO

from fala.artifacts import (
    FileArtifactStore,
    MemoryArtifactStore,
    content_address_file,
    digest_from_fala_artifact_uri,
    local_path_from_uri,
    resolve_artifact_local_path,
    sha256_digest,
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


class ContentAddressTests(unittest.TestCase):
    """Public content-address helpers stay byte-identical to what the stores stamp.

    A caller that must hash a payload *before* it enters an artifact store -- and
    so has no ref to read ``metadata['sha256']`` from -- gets the exact digest
    Fala would stamp, so a hand-hashed value and a stored ref never diverge.
    """

    def test_content_address_file_matches_stored_blob_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "payload.bin"
            source.write_bytes(b"attestation payload \x00\xff")
            ref = FileArtifactStore(root / "artifacts").put_file(
                kind="report", path=source
            )
            address = content_address_file(source)
            self.assertEqual(address, ref.metadata["sha256"])
            self.assertEqual(address, digest_from_fala_artifact_uri(ref.uri))

    def test_sha256_digest_matches_memory_store_digest(self) -> None:
        data = b"canonical bytes \x00\x01\x02"
        ref = MemoryArtifactStore().put_fileobj(
            kind="report", fileobj=BytesIO(data), filename="payload.bin"
        )
        self.assertEqual(sha256_digest(data), ref.metadata["sha256"])

    def test_file_and_bytes_helpers_agree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            data = b"same bytes, two doors"
            source = Path(tmp_dir) / "payload.bin"
            source.write_bytes(data)
            self.assertEqual(content_address_file(source), sha256_digest(data))

    def test_sha256_digest_is_known_vector(self) -> None:
        # sha256("abc") -- a fixed vector so a refactor can't silently swap the algorithm.
        self.assertEqual(
            sha256_digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )


if __name__ == "__main__":
    unittest.main()
