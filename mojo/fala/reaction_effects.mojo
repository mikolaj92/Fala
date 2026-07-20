"""Native reaction accumulation and explicit local-file materialization.

The generic accumulation and persistence APIs consume strict JSON reaction
objects and remain metadata-only.  The separate local materialization API
explicitly reads one caller-supplied regular file into the local CAS before
recording its durable reaction row; it never invokes an effector or transport.
"""

from std.collections import List
from std.pathlib import Path
from emberjson import Array, Object, Value, to_string
from fala.domain import Reaction
from fala.domain_store import NativeDomainStore
from fala.journal import EventInput
from fala.reactions import FileReactionStore, sha256_raw_bytes
from fala.json import canonical_json_text
comptime REACTION_EFFECT_UNAVAILABLE = "reaction.effect.unavailable"
comptime REACTION_EFFECT_READY = "reaction.effect.ready"
comptime REACTION_EFFECT_PERSISTED = "reaction.effect.persisted"


@fieldwise_init
struct ReactionEffectResult(Copyable, Movable):
    """Accumulated effects and their durable projection outcome."""
    var status: String
    var effects_json: String
    var persisted_count: Int
    var unavailable_code: String

    @staticmethod
    def unavailable() -> ReactionEffectResult:
        return ReactionEffectResult(
            status="unavailable", effects_json="[]", persisted_count=0,
            unavailable_code=REACTION_EFFECT_UNAVAILABLE,
        )


def _contains(values: List[String], value: String) -> Bool:
    for item in values:
        if item == value:
            return True
    return False


def _accepted(kind: String, accepted_kinds: List[String]) -> Bool:
    return len(accepted_kinds) == 0 or _contains(accepted_kinds, kind)


def _reaction_array(value: Value, source: String) raises -> Array:
    if not value.is_array():
        raise Error("malformed reaction array: " + source)
    var result = Array(capacity=len(value.array()))
    for item in value.array():
        if not item.is_object():
            raise Error("malformed reaction object: " + source)
        if "kind" not in item.object() or not item.object()["kind"].is_string():
            raise Error("reaction kind must be a string: " + source)
        if item.object()["kind"].string() == "":
            raise Error("reaction kind must not be empty: " + source)
        if "uri" in item.object() and not item.object()["uri"].is_string():
            raise Error("reaction uri must be a string: " + source)
        if "id" in item.object() and not item.object()["id"].is_string():
            raise Error("reaction id must be a string: " + source)
        result.append(item.copy())
    return result^


def _extract_reactions(value: Value, source: String) raises -> Array:
    """Read either a raw reaction array or an output envelope."""
    if value.is_array():
        var copied = value.copy()
        return _reaction_array(copied^, source)
    if not value.is_object():
        raise Error("malformed reaction output: " + source)
    # Effector envelopes may legitimately omit reactions; the reference
    # projection treats that as an empty reaction list rather than a failure.
    if "reactions" not in value.object():
        return Array(capacity=0)
    var reactions = value.object()["reactions"].copy()
    return _reaction_array(reactions^, source)


def accumulate_reaction_effects(
    upstream_outputs: List[String],
    accepted_kinds: List[String] = List[String](),
    ancestor_ids: List[String] = List[String](),
) raises -> ReactionEffectResult:
    """Accumulate accepted reactions in caller-supplied ancestor order.

    Every input is validated before filtering, so malformed upstream output is
    never silently converted into an empty effect.  The output is canonical
    JSON and retains each accepted reaction's source ancestor as metadata.
    """
    if len(ancestor_ids) > 0 and len(ancestor_ids) != len(upstream_outputs):
        raise Error("ancestor ids must match upstream output count")
    for accepted in accepted_kinds:
        if accepted == "":
            raise Error("accepted reaction kind must not be empty")
    var effects = Array(capacity=0)
    var effect_index = 0
    for output_index in range(len(upstream_outputs)):
        var source = String(output_index)
        if len(ancestor_ids) > 0:
            source = ancestor_ids[output_index]
            if source == "":
                raise Error("ancestor id must not be empty")
        var parsed = Value(parse_string=upstream_outputs[output_index])
        var reactions = _extract_reactions(parsed^, source)
        for reaction in reactions:
            var kind = reaction.object()["kind"].string()
            if not _accepted(kind, accepted_kinds):
                continue
            var effect = Object(capacity=len(reaction.object()) + 2)
            for pair in reaction.object().items():
                effect[pair.key] = pair.value.copy()
            effect["ancestor"] = Value(source)
            effect["ordinal"] = Value(effect_index)
            effects.append(Value(effect^))
            effect_index += 1
    var canonical = canonical_json_text(to_string(Value(effects^)))
    if effect_index == 0:
        return ReactionEffectResult(status="unavailable", effects_json=canonical, persisted_count=0, unavailable_code=REACTION_EFFECT_UNAVAILABLE)
    return ReactionEffectResult(status="ready", effects_json=canonical, persisted_count=0, unavailable_code="")


def persist_reaction_effects(
    mut store: NativeDomainStore,
    run_id: String,
    impulse_id: String,
    effects_json: String,
    created_at: String,
) raises -> ReactionEffectResult:
    """Persist representable effects as immutable reaction rows.

    An explicit run is required.  Only upstream objects with an existing URI
    and id can become durable reaction rows; no URI or blob is invented.  All
    writes use ``NativeDomainStore.put_reaction`` and therefore retain its
    idempotency/conflict behavior.
    """
    if run_id == "" or effects_json == "":
        return ReactionEffectResult.unavailable()
    var run_check = store.db.query("SELECT 1 FROM runs WHERE id=?")
    run_check.bind_text(1, run_id)
    if not run_check.step():
        return ReactionEffectResult.unavailable()
    if impulse_id == "":
        return ReactionEffectResult.unavailable()
    if created_at == "":
        return ReactionEffectResult.unavailable()
    var impulse_check = store.db.query("SELECT 1 FROM impulses WHERE run_id=? AND id=?")
    impulse_check.bind_text(1, run_id)
    impulse_check.bind_text(2, impulse_id)
    if not impulse_check.step():
        return ReactionEffectResult.unavailable()
    var parsed = Value(parse_string=effects_json)
    var effects = _reaction_array(parsed^, "effects")
    var persisted = 0
    for effect in effects:
        var obj = effect.object().copy()
        if "uri" not in obj or not obj["uri"].is_string() or obj["uri"].string() == "":
            continue
        if "id" not in obj or not obj["id"].is_string() or obj["id"].string() == "":
            continue
        var size_bytes = -1
        if "size_bytes" in obj:
            if not obj["size_bytes"].is_int() and not obj["size_bytes"].is_uint():
                raise Error("reaction size_bytes must be an integer")
            size_bytes = Int(obj["size_bytes"].int()) if obj["size_bytes"].is_int() else Int(obj["size_bytes"].uint())
            if size_bytes < 0:
                raise Error("reaction size_bytes must not be negative")
        var metadata = Object(capacity=len(obj) + 1)
        for pair in obj.items():
            metadata[pair.key] = pair.value.copy()
        metadata["effect"] = Value(True)
        var row = Reaction(
            id=obj["id"].string(), run_id=run_id,
            kind=obj["kind"].string(), uri=obj["uri"].string(),
            impulse_id=impulse_id, size_bytes=size_bytes,
            content_hash=(obj["content_hash"].string() if "content_hash" in obj and obj["content_hash"].is_string() else ""),
            metadata=canonical_json_text(to_string(Value(metadata^))),
            created_at=created_at,
        )
        store.put_reaction(row)
        persisted += 1
    if persisted == 0:
        return ReactionEffectResult(status="unavailable", effects_json=effects_json, persisted_count=0, unavailable_code=REACTION_EFFECT_UNAVAILABLE)
    return ReactionEffectResult(status="persisted", effects_json=effects_json, persisted_count=persisted, unavailable_code="")
@fieldwise_init
struct LocalReactionEffectResult(Copyable, Movable):
    """Durable result of one explicit local-file reaction materialization."""
    var status: String
    var reaction_json: String
    var replayed: Bool
    var unavailable_code: String
    var error_code: String

    @staticmethod
    def unavailable() -> LocalReactionEffectResult:
        return LocalReactionEffectResult(
            status="unavailable", reaction_json="null", replayed=False,
            unavailable_code=REACTION_EFFECT_UNAVAILABLE, error_code="",
        )


def _local_value_is_safe(value: String, label: String) raises:
    if value == "":
        raise Error(label + " must not be empty")
    if value.find("\0") >= 0 or value.find("\n") >= 0 or value.find("\r") >= 0:
        raise Error(label + " contains NUL or newline")


def _local_metadata(blob_metadata: String, caller_metadata: String, reaction_root: String) raises -> String:
    var blob = Value(parse_string=blob_metadata)
    if not blob.is_object():
        raise Error("reaction CAS metadata must be an object")
    var caller = Value(parse_string=caller_metadata)
    if not caller.is_object():
        raise Error("reaction metadata must be an object")
    var merged = Object(capacity=len(blob.object()) + len(caller.object()) + 1)
    for pair in caller.object().items():
        merged[pair.key] = pair.value.copy()
    for pair in blob.object().items():
        merged[pair.key] = pair.value.copy()
    merged["reaction_store"] = Value(reaction_root)
    return canonical_json_text(to_string(Value(merged^)))
def _validate_local_metadata_identity(metadata_json: String, blob_uri: String, digest: String, size_bytes: Int) raises:
    var supplied = Value(parse_string=metadata_json)
    if not supplied.is_object():
        raise Error("reaction metadata must be an object")
    if "uri" in supplied.object():
        if not supplied.object()["uri"].is_string() or supplied.object()["uri"].string() != blob_uri:
            raise Error("reaction CAS URI mismatch")
    if "content_hash" in supplied.object():
        if not supplied.object()["content_hash"].is_string():
            raise Error("reaction CAS content hash mismatch")
        var supplied_hash = supplied.object()["content_hash"].string()
        if supplied_hash != "sha256:" + digest:
            raise Error("reaction CAS content hash mismatch")
    if "size_bytes" in supplied.object():
        var supplied_size = supplied.object()["size_bytes"].copy()
        if (not supplied_size.is_int()) and (not supplied_size.is_uint()):
            raise Error("reaction CAS size mismatch")
        var size = Int(supplied_size.int()) if supplied_size.is_int() else Int(supplied_size.uint())
        if size != size_bytes:
            raise Error("reaction CAS size mismatch")


def materialize_local_reaction_effect(
    mut store: NativeDomainStore,
    reaction_root: String,
    source_path: String,
    run_id: String,
    impulse_id: String,
    kind: String,
    timestamp: String,
    reaction_id: String = "",
    media_type: String = "",
    expected_uri: String = "",
    expected_content_hash: String = "",
    expected_size_bytes: Int = -1,
    metadata_json: String = "{}",
) raises -> LocalReactionEffectResult:
    """Materialize exactly one local regular file into CAS and record it.

    This is deliberately a local filesystem boundary: it never invokes an
    effector or transport.  Existing CAS blobs are verified by ``put_bytes_raw``
    before any durable reaction row is committed.
    """
    _local_value_is_safe(reaction_root, "reaction root")
    _local_value_is_safe(source_path, "source path")
    _local_value_is_safe(run_id, "run id")
    _local_value_is_safe(impulse_id, "impulse id")
    _local_value_is_safe(kind, "reaction kind")
    _local_value_is_safe(timestamp, "reaction timestamp")
    if expected_size_bytes < -1:
        raise Error("expected size must be non-negative")
    var existing_run = True
    try:
        _ = store.list_reaction_records(run_id)
        _ = store.get_impulse(run_id, impulse_id)
    except err:
        existing_run = False
    if not existing_run:
        return LocalReactionEffectResult.unavailable()
    var source = Path(source_path)
    if not source.exists() or not source.is_file():
        raise Error("reaction source path must be a regular file")
    var content = source.read_bytes()
    var digest = sha256_raw_bytes(content.copy())
    var size_bytes = len(content)
    var expected_uri_value = "fala-reaction://sha256/" + digest
    _validate_local_metadata_identity(metadata_json, expected_uri_value, digest, size_bytes)
    if expected_uri != "" and expected_uri != expected_uri_value:
        raise Error("reaction CAS URI mismatch")
    if expected_content_hash != "" and expected_content_hash != "sha256:" + digest:
        raise Error("reaction CAS content hash mismatch")
    if expected_size_bytes >= 0 and expected_size_bytes != size_bytes:
        raise Error("reaction CAS size mismatch")
    var id = reaction_id
    if id == "": id = "reaction:" + digest
    _local_value_is_safe(id, "reaction id")
    var reaction_store = FileReactionStore(reaction_root)
    var blob = reaction_store.put_bytes_raw(content.copy(), source.name(), metadata_json)
    _validate_local_metadata_identity(metadata_json, blob.uri, blob.digest, blob.size_bytes)
    var metadata = _local_metadata(blob.metadata, metadata_json, reaction_store.location())
    var row = Reaction(
        id=id, run_id=run_id, kind=kind, uri=blob.uri, impulse_id=impulse_id,
        media_type=media_type, size_bytes=blob.size_bytes,
        content_hash="sha256:" + blob.digest, metadata=metadata, created_at=timestamp,
    )
    var key = "reaction.record:" + id
    var events = List[EventInput]()
    events.append(EventInput(
        id=key + ":event", event_type="reaction.recorded", payload=row.to_json(),
        created_at=timestamp, impulse_id=impulse_id, process_id="", schema_version=1,
        actor="", correlation_id="", causation_id="",
    ))
    var submission = store.record_reaction(row, key, "reaction.record", key, timestamp, events)
    var result_row = row.copy()
    if submission.replayed:
        result_row = store.get_reaction(run_id, id)
    return LocalReactionEffectResult(
        status="replayed" if submission.replayed else "persisted",
        reaction_json=result_row.to_json(), replayed=submission.replayed,
        unavailable_code="", error_code="",
    )
