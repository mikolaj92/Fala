from std.collections import List
from fala.reactions import put_bytes, resolve_uri, is_fala_reaction_uri, digest_from_fala_reaction_uri, content_address_json, sha256_raw_bytes, FileReactionStore, _sha256_bit_length

def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("reaction parity smoke: " + message)

def main() raises:
    _check(_sha256_bit_length(0x1fffffffffffffff) == UInt64(0xfffffffffffffff8), "maximum SHA-256 byte length")
    var oversized_sha256_length_rejected = False
    try:
        _ = _sha256_bit_length(0x2000000000000000)
    except:
        oversized_sha256_length_rejected = True
    _check(oversized_sha256_length_rejected, "oversized SHA-256 byte length rejected")
    var negative_sha256_length_rejected = False
    try:
        _ = _sha256_bit_length(-1)
    except:
        negative_sha256_length_rejected = True
    _check(negative_sha256_length_rejected, "negative SHA-256 byte length rejected")

    var empty_bytes = List[UInt8]()
    _check(sha256_raw_bytes(empty_bytes^) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "SHA-256 empty input")
    var abc_bytes = List[UInt8]()
    abc_bytes.append(UInt8(97)); abc_bytes.append(UInt8(98)); abc_bytes.append(UInt8(99))
    _check(sha256_raw_bytes(abc_bytes^) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", "SHA-256 abc")
    var binary_bytes = List[UInt8]()
    binary_bytes.append(UInt8(0)); binary_bytes.append(UInt8(128)); binary_bytes.append(UInt8(255))
    _check(sha256_raw_bytes(binary_bytes^) == "5240672d7b51756b829ad0ef8d9468b7a078afa2f410484fd3892dab47becb72", "SHA-256 exact binary bytes")
    var block_text = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
    var block_bytes = List[UInt8]()
    for index in range(block_text.byte_length()):
        block_bytes.append(block_text.as_bytes()[index])
    _check(sha256_raw_bytes(block_bytes^) == "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1", "SHA-256 multi-block boundary")

    var root = "/tmp/fala-reaction-native-smoke"
    var store = FileReactionStore(root)
    _check(store.location() == root, "reaction store reports configured root")
    var existing = store.list_blob_digests()
    if len(existing) > 0:
        _ = store.delete_blobs(existing^)
    var kept = put_bytes(root, "a", "demo.txt")
    var removed = put_bytes(root, "b", "removed.txt")
    var orphan = put_bytes(root, "c", "orphan.txt")
    _check(kept.size_bytes == 1 and kept.metadata.find("\"sha256\":\"" + kept.digest + "\"") >= 0 and kept.metadata.find("\"size_bytes\":1") >= 0 and kept.metadata.find("\"content_addressed\":true") >= 0, "persisted CAS metadata and byte accounting")
    var merged = store.put_bytes_with_metadata("meta", "meta.txt", "{\"z\":1,\"a\":{\"nested\":true}}")
    _check(merged.metadata.find("\"a\":{\"nested\":true}") >= 0 and merged.metadata.find("\"z\":1") >= 0 and merged.metadata.find("\"sha256\":\"") >= 0, "caller metadata canonicalized with reserved CAS fields")
    print(kept.digest)
    print(resolve_uri(root, kept.uri))
    _check(is_fala_reaction_uri(kept.uri), "strict URI accepted")
    _check(not is_fala_reaction_uri("fala-reaction://sha256/" + kept.digest.upper()), "uppercase digest URI rejected")
    var uppercase_digest_rejected = False
    try:
        _ = digest_from_fala_reaction_uri("fala-reaction://sha256/" + kept.digest.upper())
    except:
        uppercase_digest_rejected = True
    _check(uppercase_digest_rejected, "uppercase digest extraction rejected")
    _check(not is_fala_reaction_uri("fala-reaction://sha256/not-a-digest"), "malformed URI rejected")
    _check(not is_fala_reaction_uri("fala-reaction://sha256/" + kept.digest + "?extra=1"), "URI query rejected")
    _check(digest_from_fala_reaction_uri(kept.uri) == kept.digest, "digest extraction")
    var invalid_digest_rejected = False
    try:
        _ = digest_from_fala_reaction_uri("fala-reaction://sha256/abc")
    except:
        invalid_digest_rejected = True
    _check(invalid_digest_rejected, "invalid digest raises")
    var listed = store.list_blob_digests()
    var kept_seen = False
    var removed_seen = False
    var orphan_seen = False
    for digest in listed:
        if digest == kept.digest: kept_seen = True
        if digest == removed.digest: removed_seen = True
        if digest == orphan.digest: orphan_seen = True
    _check(kept_seen and removed_seen and orphan_seen, "stored digests listed")
    for index in range(1, len(listed)):
        _check(listed[index - 1] <= listed[index], "blob listing is deterministic")
    var duplicate_delete = List[String]()
    duplicate_delete.append(removed.digest)
    duplicate_delete.append(removed.digest)
    var deleted = store.delete_blobs(duplicate_delete^)
    _check(len(deleted) == 1 and deleted[0] == removed.digest, "duplicate delete is idempotent")
    var references = List[String](); references.append(kept.digest); references.append(merged.digest)
    var candidates = store.collect_garbage(references^, dry_run=True)
    _check(len(candidates) == 1 and candidates[0] == orphan.digest, "GC identifies orphan deterministically")
    var collected = store.collect_garbage(references^, dry_run=False)
    _check(len(collected) == 1 and collected[0] == orphan.digest, "GC deletion preserves reference")
    _check(store.resolve(kept.uri) != "", "referenced blob remains resolvable")
    # Fixed SHA-256 values generated by Python's
    # json.dumps(sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    # for the shared scalar/string subset.  The API accepts JSON text rather
    # than a structured payload; EmberJson's native Float64 spelling is not
    # promised to match Python for every exponent or negative-zero input.
    _check(content_address_json("{\"b\":1,\"a\":[\"ż\",2]}") == "64518e6e774d821af6e21ae142daaa74e778d90bfe92965c286534aeb7603882", "nested key order and unicode")
    _check(content_address_json("{\"z\":{\"b\":[true,null,3.5],\"a\":\"ż\"},\"a\":1}") == "cd94373f7ed0b4c0df95969e585cb727fb41c6d4e9379e23941a7292c6beaf3d", "nested arrays and null")
    _check(content_address_json("{\"text\":\"héllo 世界 🌍\",\"empty\":\"\"}") == "a0fd782b00c62dcbe6d7cdfd4b34350da3a8f564553bec1a64964329f0e8b02d", "unicode strings")
    _check(content_address_json("{\"bool\":true,\"null\":null,\"integer\":42,\"negative\":-7,\"float\":3.5}") == "0eecc792bea223c87f706c0e1f92e9a2ed7bd7a72cb818b1949e4c640a483a50", "scalar types")
    _check(content_address_json("[null,false,0,1.0,\"x\",{\"b\":2,\"a\":1}]") == "00a16bc8b7a1fbadedf4b380908fd690be6aa5c08aa02392c7058a8d6b1a49c0", "array and float")
    _check(content_address_json("{\"controls\":\"line\\nnext\",\"escaped\":\"quote \\\" and \\\\ slash\"}") == "355a3a7eba556ca6da5247fa3101b9eb7d7d1fe31bd845a5e81cfa274eb45c58", "JSON escaping")
    print("reaction CAS/GC parity smoke ok")
