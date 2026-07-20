"""Native filesystem content-addressed reaction storage.

The public boundary intentionally accepts UTF-8 Mojo ``String`` values.  SHA-256
is provided by the platform CommonCrypto C ABI on Darwin; no Python runtime is
loaded.  Files are written to a temporary sibling and committed with POSIX
``rename`` so readers never observe a partial blob.
"""

from std.collections import List
from std.ffi import CStringSlice, c_int, c_uint, external_call
from std.memory import UnsafePointer, alloc
from std.os import listdir, makedirs, remove
from std.pathlib import Path, cwd
from emberjson import Array, Object, Value, to_string
from fala.json import canonical_json_text

comptime FALA_REACTION_SCHEME = "fala-reaction"
comptime _URI_PREFIX = "fala-reaction://sha256/"
comptime _HEX = "0123456789abcdef"


struct ReactionBlob:
    var digest: String
    var size_bytes: Int
    var uri: String
    var metadata: String

    def __init__(out self, digest: String, size_bytes: Int, filename: String = "reaction", metadata_json: String = ""):
        self.digest = digest
        self.size_bytes = size_bytes
        self.uri = _URI_PREFIX + digest
        if metadata_json == "":
            self.metadata = _reaction_metadata(digest, size_bytes, filename)
        else:
            self.metadata = metadata_json

@fieldwise_init
struct ReactionBlobInfo(Copyable, Movable):
    """Verified native blob location and exact byte size."""
    var digest: String
    var location: String
    var size_bytes: Int

@fieldwise_init
struct ReactionStoreUnavailable(Copyable, Movable):
    """Typed diagnostic for reaction-store operations unavailable in Mojo."""
    var code: String
    var message: String

    @staticmethod
    def fileobj() -> ReactionStoreUnavailable:
        return ReactionStoreUnavailable(
            code="reaction.put_fileobj_unavailable",
            message="put_fileobj requires a host file-object protocol unavailable in this Mojo runtime",
        )

    def is_unavailable(self) -> Bool:
        return self.code != ""

    def __str__(self) -> String:
        return self.message

def put_fileobj_unavailable() -> ReactionStoreUnavailable:
    """Return a typed diagnostic because Mojo has no portable file-object API."""
    return ReactionStoreUnavailable.fileobj()

def _json_quote(value: String) -> String:
    var result = "\""
    for i in range(value.byte_length()):
        var ch = value[byte=i]
        if ch == "\"": result += "\\\""
        elif ch == "\\": result += "\\\\"
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        elif ch < " ":
            var byte_value = Int(String(ch).as_bytes()[0])
            result += "\\u00"
            result += _HEX[byte=byte_value >> 4]
            result += _HEX[byte=byte_value & 15]
        else: result += String(ch)
    result += "\""
    return result


def _realpath(path: Path) raises -> Path:
    """Resolve an existing path through symlinks using Darwin realpath."""
    var source_text = path.__fspath__() + "\0"
    var source_c = CStringSlice(source_text)
    var buffer = alloc[UInt8](4096)
    var resolved = external_call["realpath", UnsafePointer[UInt8, MutUntrackedOrigin]](
        source_c.unsafe_ptr(), buffer
    )
    if Int(resolved) == 0:
        buffer.free()
        raise Error("Unable to resolve reaction path")
    var resolved_text = String(unsafe_from_utf8_ptr=resolved)
    buffer.free()
    return Path(resolved_text)

def _reaction_metadata(digest: String, size_bytes: Int, filename: String) -> String:
    return "{\"filename\":" + _json_quote(filename) + ",\"sha256\":" + _json_quote(digest) + ",\"size_bytes\":" + String(size_bytes) + ",\"storage\":{\"backend\":\"file\",\"content_addressed\":true}}"

def _reaction_metadata_with_caller(digest: String, size_bytes: Int, filename: String, metadata_json: String) raises -> String:
    """Merge caller metadata while reserving CAS identity fields."""
    var source = Value(parse_string=metadata_json)
    if not source.is_object():
        raise Error("Reaction metadata must be a JSON object")
    var metadata = Object(capacity=len(source.object()) + 4)
    for pair in source.object().items():
        metadata[pair.key] = pair.value.copy()
    metadata["filename"] = Value(filename)
    metadata["sha256"] = Value(digest)
    metadata["size_bytes"] = Value(size_bytes)
    var storage = Object(capacity=2)
    storage["backend"] = Value("file")
    storage["content_addressed"] = Value(True)
    metadata["storage"] = Value(storage^)
    return canonical_json_text(to_string(Value(metadata^)))

def _sha256_raw_bytes(bytes: List[UInt8]) raises -> String:
    var output = alloc[UInt8](32)
    var input = bytes.unsafe_ptr()
    _ = external_call["CC_SHA256", UnsafePointer[UInt8, MutUntrackedOrigin]](
        input, c_uint(len(bytes)), output
    )
    var digest = String()
    for i in range(32):
        var value = output[i]
        digest += _HEX[byte=Int(value >> 4)]
        digest += _HEX[byte=Int(value & 15)]
    output.free()
    return digest


def sha256_bytes(content: String) raises -> String:
    """Return the lowercase SHA-256 digest of UTF-8 ``content``."""
    var bytes = List[UInt8]()
    for i in range(content.byte_length()):
        bytes.append(content.as_bytes()[i])
    return _sha256_raw_bytes(bytes^)
def content_address_json(json_text: String) raises -> String:
    """Hash canonical JSON text using the native EmberJson representation.

    This boundary accepts a JSON document, not a structured Mojo/Python value.
    It sorts object keys recursively and hashes UTF-8 output.  Python parity is
    guaranteed for the shared scalar/string subset covered by the fixed smoke
    vectors; callers must not assume Python ``json.dumps`` formatting for every
    Float64 spelling (notably ``-0.0`` and exponent notation).
    """
    return sha256_bytes(canonical_json_text(json_text))
def _utf8_bytes(content: String) raises -> List[UInt8]:
    var bytes = List[UInt8]()
    for i in range(content.byte_length()):
        bytes.append(content.as_bytes()[i])
    return bytes^
def sha256_raw_bytes(content: List[UInt8]) raises -> String:
    """Return SHA-256 for exact bytes without a UTF-8 String contract."""
    return _sha256_raw_bytes(content.copy())


def _absolute_root(root: String) raises -> Path:
    var expanded = Path(root).expanduser()
    if root.startswith("/"):
        return expanded
    return cwd() / expanded


def _blob_root(root: String) raises -> Path:
    return _absolute_root(root) / "blobs" / "sha256"


def _digest_valid(digest: String) -> Bool:
    if digest.byte_length() != 64:
        return False
    for i in range(64):
        var ch = digest[byte=i]
        if not (
            (ch >= "0" and ch <= "9")
            or (ch >= "a" and ch <= "f")
            or (ch >= "A" and ch <= "F")
        ):
            return False
    return True


def _lower_hex(digest: String) -> String:
    var result = String()
    for i in range(digest.byte_length()):
        var ch = digest[byte=i]
        if ch >= "A" and ch <= "F":
            result += String(ch).lower()
        else:
            result += ch
    return result
def reaction_uri_from_digest(digest: String) raises -> String:
    """Build a strict reaction URI from a validated digest."""
    if not _digest_valid(digest):
        raise Error("Invalid reaction digest")
    return _URI_PREFIX + _lower_hex(digest)


def _digest_from_uri(uri: String) raises -> String:
    if not uri.startswith(_URI_PREFIX) or uri.byte_length() != _URI_PREFIX.byte_length() + 64:
        raise Error("Invalid Fala reaction URI")
    var digest = String(uri[byte=_URI_PREFIX.byte_length():])
    if not _digest_valid(digest):
        raise Error("Invalid Fala reaction digest")
    return _lower_hex(digest)


def _verify_blob_digest(target: Path, digest: String) raises:
    try:
        # Hash exact bytes; decoding a blob as text corrupts arbitrary binary.
        var existing = target.read_bytes()
        if _sha256_raw_bytes(existing) != digest:
            raise Error("Stored reaction blob digest mismatch")
    except err:
        raise Error("Stored reaction blob digest mismatch")




def is_fala_reaction_uri(uri: String) -> Bool:
    """Return whether ``uri`` is a strict SHA-256 reaction URI."""
    try:
        _ = _digest_from_uri(uri)
        return True
    except:
        return False


def digest_from_fala_reaction_uri(uri: String) raises -> String:
    """Extract and normalize the lowercase digest from a reaction URI."""
    return _digest_from_uri(uri)
def reaction_digest_or_empty(uri: String) -> String:
    """Return a normalized CAS digest, or empty for a non-CAS URI."""
    try:
        return _digest_from_uri(uri)
    except:
        return String("")
def filter_reactions_json(output_json: String, accepted_kinds: List[String] = List[String]()) raises -> String:
    """Return canonical reaction objects from an effector output envelope."""
    var output = Value(parse_string=output_json)
    if not output.is_object() or "reactions" not in output.object():
        return "[]"
    var reactions = output.object()["reactions"].copy()
    if not reactions.is_array():
        return "[]"
    var filtered = Array(capacity=len(reactions.array()))
    for reaction in reactions.array():
        if not reaction.is_object():
            continue
        if len(accepted_kinds) == 0:
            filtered.append(reaction.copy())
            continue
        if "kind" not in reaction.object():
            continue
        var kind = reaction.object()["kind"].copy()
        if not kind.is_string():
            continue
        for accepted in accepted_kinds:
            if kind.string() == accepted:
                filtered.append(reaction.copy())
                break
    return canonical_json_text(to_string(Value(filtered^)))

def _path_inside(path: Path, root: Path) raises -> Bool:
    var root_resolved = _realpath(root) if root.exists() else root
    var path_resolved = _realpath(path) if path.exists() else path
    var path_text = path_resolved.__fspath__()
    var root_text = root_resolved.__fspath__()
    if path_text == root_text:
        return True
    return path_text.startswith(root_text + "/")

def _atomic_rename(source: Path, target: Path) raises:
    # CStringSlice requires a nul-terminated view; both strings stay alive for
    # the duration of the external call.
    var source_text = source.__fspath__() + "\0"
    var target_text = target.__fspath__() + "\0"
    var source_c = CStringSlice(source_text)
    var target_c = CStringSlice(target_text)
    var result = external_call["rename", c_int](source_c, target_c)
    if result != 0:
        raise Error("Atomic reaction blob rename failed")


def _put_raw_bytes(root: String, content: List[UInt8], filename: String, metadata_json: String = "") raises -> ReactionBlob:
    """Atomically persist exact bytes and return their content address."""
    var size_bytes = len(content)
    var digest = _sha256_raw_bytes(content.copy())
    var root_path = _absolute_root(root)
    var blob_root = root_path / "blobs" / "sha256"
    var shard = blob_root / digest[byte=0:2]
    var target = shard / digest
    var temp_root = root_path / "tmp"
    makedirs(shard, exist_ok=True)
    makedirs(temp_root, exist_ok=True)
    if not _path_inside(temp_root, root_path):
        raise Error("Reaction path escapes reaction store root")
    if not _path_inside(shard, root_path):
        raise Error("Reaction path escapes reaction store root")
    if target.exists():
        if not _path_inside(target, root_path):
            raise Error("Reaction path escapes reaction store root")
        if not target.is_file():
            raise Error("Reaction blob path is not a file")
        _verify_blob_digest(target, digest)
        var metadata = _reaction_metadata(digest, size_bytes, filename)
        if metadata_json != "": metadata = _reaction_metadata_with_caller(digest, size_bytes, filename, metadata_json)
        return ReactionBlob(digest, size_bytes, filename, metadata)
    var temp_template = temp_root.__fspath__() + "/reaction-" + digest + "-XXXXXX\0"
    var temp_c = CStringSlice(temp_template)
    var temp_fd = external_call["mkstemp", c_int](temp_c.unsafe_ptr())
    if temp_fd < 0:
        raise Error("Unable to create temporary reaction blob")
    _ = external_call["close", c_int](temp_fd)
    var temp = Path(String(temp_template[byte=0:temp_template.byte_length() - 1]))
    temp.write_bytes(content.copy())
    _atomic_rename(temp, target)
    var final_metadata = _reaction_metadata(digest, size_bytes, filename)
    if metadata_json != "": final_metadata = _reaction_metadata_with_caller(digest, size_bytes, filename, metadata_json)
    return ReactionBlob(digest, size_bytes, filename, final_metadata)


def put_bytes(root: String, content: String, filename: String) raises -> ReactionBlob:
    """Atomically store UTF-8 content and return its content address."""
    return _put_raw_bytes(root, _utf8_bytes(content), filename)


def put_bytes_with_metadata(root: String, content: String, filename: String, metadata_json: String) raises -> ReactionBlob:
    """Store UTF-8 content and merge caller JSON metadata with CAS fields."""
    return _put_raw_bytes(root, _utf8_bytes(content), filename, metadata_json)
def put_file(root: String, path: String, filename: String = "", metadata_json: String = "") raises -> ReactionBlob:
    """Atomically store exact bytes from a regular source file."""
    var source = Path(path).expanduser()
    if not source.exists() or not source.is_file():
        raise Error("Reaction source path must be a regular file")
    var content = source.read_bytes()
    var stored_name = filename
    if stored_name == "": stored_name = source.name()
    return _put_raw_bytes(root, content^, stored_name, metadata_json)



def put_bytes_raw(root: String, content: List[UInt8], filename: String = "reaction", metadata_json: String = "") raises -> ReactionBlob:
    """Store exact raw bytes; unlike ``put_bytes``, no UTF-8 decoding occurs."""
    return _put_raw_bytes(root, content.copy(), filename, metadata_json)


def resolve_uri(root: String, uri: String) raises -> String:
    """Resolve a validated reaction URI to an existing local blob path."""
    var digest = _digest_from_uri(uri)
    var root_path = _absolute_root(root)
    var blob_root = root_path / "blobs" / "sha256"
    var target = blob_root / digest[byte=0:2] / digest
    if not target.exists() or not target.is_file():
        raise Error("Stored reaction blob not found")
    if not _path_inside(target, root_path):
        raise Error("Reaction path escapes reaction store root")
    _verify_blob_digest(target, digest)
    return _realpath(target).__fspath__()

def _remove_empty_shard(shard: Path) raises:
    """Best-effort cleanup for an empty digest shard after deletion."""
    if not shard.exists() or not shard.is_dir():
        return
    var shard_text = shard.__fspath__() + "\0"
    var shard_c = CStringSlice(shard_text)
    _ = external_call["rmdir", c_int](shard_c.unsafe_ptr())

def _sorted_unique_digests(digests: List[String]) raises -> List[String]:
    """Validate, normalize, deduplicate, and lexically order digest inputs."""
    var result = List[String]()
    for digest_input in digests:
        if not _digest_valid(digest_input):
            raise Error("Invalid reaction digest")
        var digest = _lower_hex(digest_input)
        var already_present = False
        for existing in result:
            if existing == digest:
                already_present = True
                break
        if not already_present:
            result.append(digest)
    var index = 1
    while index < len(result):
        var current = result[index]
        var position = index
        while position > 0 and result[position - 1] > current:
            result[position] = result[position - 1]
            position -= 1
        result[position] = current
        index += 1
    return result^
struct FileReactionStore:
    var root: String

    def __init__(out self, root: String) raises:
        var path = _absolute_root(root)
        self.root = path.__fspath__()
        makedirs(path / "blobs" / "sha256", exist_ok=True)

    def location(self) -> String:
        return self.root

    def put_bytes(self, content: String, filename: String = "reaction") raises -> ReactionBlob:
        return put_bytes(self.root, content, filename)
    def put_file(self, path: String, filename: String = "", metadata_json: String = "") raises -> ReactionBlob:
        return put_file(self.root, path, filename, metadata_json)

    def put_fileobj_unavailable(self) -> ReactionStoreUnavailable:
        """Return the typed diagnostic for unsupported host file objects."""
        return ReactionStoreUnavailable.fileobj()

    def put_bytes_with_metadata(self, content: String, filename: String = "reaction", metadata_json: String = "{}") raises -> ReactionBlob:
        return put_bytes_with_metadata(self.root, content, filename, metadata_json)

    def put_bytes_raw(self, content: List[UInt8], filename: String = "reaction", metadata_json: String = "") raises -> ReactionBlob:
        return put_bytes_raw(self.root, content.copy(), filename, metadata_json)

    def resolve(self, uri: String) raises -> String:
        return resolve_uri(self.root, uri)

    def list_blob_digests(self) raises -> List[String]:
        """Return valid stored digests in deterministic lexical order."""
        var result = List[String]()
        for location in self.list_blobs():
            result.append(Path(location).name())
        return result^

    def list_blobs(self) raises -> List[String]:
        """Return verified blob paths in deterministic lexical order."""
        var result = List[String]()
        var blob_root = _blob_root(self.root)
        if not blob_root.exists():
            return result^
        for shard_entry in blob_root.listdir():
            var shard = blob_root / shard_entry.name()
            var shard_name = shard.name()
            if not shard.is_dir() or shard_name.byte_length() != 2 or shard_name != _lower_hex(shard_name):
                continue
            for candidate_entry in shard.listdir():
                var candidate = shard / candidate_entry.name()
                var digest = candidate.name()
                if not candidate.is_file() or not _digest_valid(digest):
                    continue
                if digest != _lower_hex(digest) or digest[byte=0:2] != shard_name:
                    continue
                _verify_blob_digest(candidate, digest)
                result.append(candidate.__fspath__())
        var index = 1
        while index < len(result):
            var current = result[index]

            var position = index
            while position > 0 and result[position - 1] > current:
                result[position] = result[position - 1]
                position -= 1
            result[position] = current
            index += 1
        return result^
    def list_blob_info(self) raises -> List[ReactionBlobInfo]:
        """Return verified blob metadata without changing legacy list_blobs output."""
        var result = List[ReactionBlobInfo]()
        for location in self.list_blobs():
            var path = Path(location)
            var digest = path.name()
            result.append(ReactionBlobInfo(digest=digest, location=location, size_bytes=len(path.read_bytes())))
        return result^

    def delete_blobs(self, digests: List[String]) raises -> List[String]:
        """Delete verified blobs in normalized lexical order."""
        var deleted = List[String]()
        var blob_root = _blob_root(self.root)
        var normalized = _sorted_unique_digests(digests.copy())
        var root_path = _absolute_root(self.root)
        for digest in normalized:
            var shard = blob_root / digest[byte=0:2]
            var target = shard / digest
            if target.exists():
                if not _path_inside(target, root_path):
                    raise Error("Reaction path escapes reaction store root")
                if not target.is_file():
                    raise Error("Stored reaction blob is not a file")
                _verify_blob_digest(target, digest)
                remove(target)
                deleted.append(digest)
                _remove_empty_shard(shard)
        return deleted^


    def collect_garbage(self, referenced_digests: List[String], dry_run: Bool = True) raises -> List[String]:
        """Return unreferenced verified blob digests, optionally deleting them."""
        var candidates = List[String]()
        var references = _sorted_unique_digests(referenced_digests.copy())
        for location in self.list_blobs():
            var digest = Path(location).name()
            var referenced = False
            for reference in references:
                if reference == digest:
                    referenced = True
                    break
            if not referenced:
                candidates.append(digest)
        if dry_run:
            return candidates^
        return self.delete_blobs(candidates)

def main():
    # Library module; direct build is a compile smoke only.
    pass
