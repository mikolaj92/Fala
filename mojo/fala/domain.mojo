"""Persistence-neutral native Fala ontology records.

Records deliberately keep object, array, and timestamp payloads as JSON-shaped
String values.  This module has no persistence or Python dependencies.
"""

from fala.json import quote_json_string as _quote

def _optional_json(value: String) -> String:
    if value == "": return "null"
    return _quote(value)

def _optional_int_json(value: Int) -> String:
    if value < 0: return "null"
    return String(value)


def _nonempty(value: String) -> Bool:
    return value.byte_length() > 0


def _status_homeostat(value: String) -> Bool:
    return value == "open" or value == "completed" or value == "cancelled" or value == "expired"


def _status_bridge(value: String) -> Bool:
    return value == "pending" or value == "delivered" or value == "imported" or value == "failed"


def bridge_status_transition_allowed(from_status: String, to_status: String) -> Bool:
    """Return whether a bridge delivery may advance to ``to_status``."""
    if from_status == to_status:
        return _status_bridge(to_status)
    if from_status != "pending":
        return False
    return to_status == "delivered" or to_status == "imported" or to_status == "failed"

struct Impulse(Copyable, Movable):
    var id: String
    var run_id: String
    var impulse_type: String
    var payload: String
    var metadata: String
    var created_at: String
    var updated_at: String

    def __init__(out self, id: String, run_id: String, impulse_type: String,
                 payload: String = "{}", metadata: String = "{}",
                 created_at: String = "", updated_at: String = ""):
        self.id = id
        self.run_id = run_id
        self.impulse_type = impulse_type
        self.payload = payload
        self.metadata = metadata
        self.created_at = created_at
        self.updated_at = updated_at

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id) and _nonempty(self.impulse_type)

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"impulse_type\":" + _quote(self.impulse_type) + ",\"payload\":" + self.payload + ",\"metadata\":" + self.metadata + ",\"created_at\":" + _quote(self.created_at) + ",\"updated_at\":" + _quote(self.updated_at) + "}"


struct ImpulseType(Copyable, Movable):
    var id: String
    var run_id: String
    var title: String
    var description: String
    var media_types: String
    var value_schema: String
    var metadata: String
    var created_at: String
    var updated_at: String

    def __init__(out self, id: String, run_id: String, title: String = "",
                 description: String = "", media_types: String = "[]",
                 value_schema: String = "{}", metadata: String = "{}",
                 created_at: String = "", updated_at: String = ""):
        self.id = id
        self.run_id = run_id
        self.title = title
        self.description = description
        self.media_types = media_types
        self.value_schema = value_schema
        self.metadata = metadata
        self.created_at = created_at
        self.updated_at = updated_at

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id)

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"title\":" + _optional_json(self.title) + ",\"description\":" + _optional_json(self.description) + ",\"media_types\":" + self.media_types + ",\"value_schema\":" + self.value_schema + ",\"metadata\":" + self.metadata + ",\"created_at\":" + _quote(self.created_at) + ",\"updated_at\":" + _quote(self.updated_at) + "}"


struct ImpulseRelation(Copyable, Movable):
    var id: String
    var run_id: String
    var relation_type: String
    var source_impulse_id: String
    var target_impulse_id: String
    var metadata: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, relation_type: String,
                 source_impulse_id: String, target_impulse_id: String,
                 metadata: String = "{}", created_at: String = ""):
        self.id = id
        self.run_id = run_id
        self.relation_type = relation_type
        self.source_impulse_id = source_impulse_id
        self.target_impulse_id = target_impulse_id
        self.metadata = metadata
        self.created_at = created_at

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id) and _nonempty(self.relation_type) and _nonempty(self.source_impulse_id) and _nonempty(self.target_impulse_id)

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"relation_type\":" + _quote(self.relation_type) + ",\"source_impulse_id\":" + _quote(self.source_impulse_id) + ",\"target_impulse_id\":" + _quote(self.target_impulse_id) + ",\"metadata\":" + self.metadata + ",\"created_at\":" + _quote(self.created_at) + "}"


struct Association(Copyable, Movable):
    var id: String
    var run_id: String
    var kind: String
    var impulse_id: String
    var values: String
    var metadata: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, kind: String,
                 impulse_id: String = "", values: String = "{}",
                 metadata: String = "{}", created_at: String = ""):
        self.id = id
        self.run_id = run_id
        self.kind = kind
        self.impulse_id = impulse_id
        self.values = values
        self.metadata = metadata
        self.created_at = created_at

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id) and _nonempty(self.kind)

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"kind\":" + _quote(self.kind) + ",\"impulse_id\":" + _optional_json(self.impulse_id) + ",\"values\":" + self.values + ",\"metadata\":" + self.metadata + ",\"created_at\":" + _quote(self.created_at) + "}"

struct Reaction(Copyable, Movable):
    var id: String
    var run_id: String
    var kind: String
    var uri: String
    var impulse_id: String
    var media_type: String
    var size_bytes: Int
    var content_hash: String
    var metadata: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, kind: String, uri: String,
                 impulse_id: String = "", media_type: String = "",
                 size_bytes: Int = -1, content_hash: String = "",
                 metadata: String = "{}", created_at: String = ""):
        self.id = id
        self.run_id = run_id
        self.kind = kind
        self.uri = uri
        self.impulse_id = impulse_id
        self.media_type = media_type
        self.size_bytes = size_bytes
        self.content_hash = content_hash
        self.metadata = metadata
        self.created_at = created_at

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id) and _nonempty(self.kind) and _nonempty(self.uri) and self.size_bytes >= -1

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"kind\":" + _quote(self.kind) + ",\"uri\":" + _quote(self.uri) + ",\"impulse_id\":" + _optional_json(self.impulse_id) + ",\"media_type\":" + _optional_json(self.media_type) + ",\"size_bytes\":" + _optional_int_json(self.size_bytes) + ",\"content_hash\":" + _optional_json(self.content_hash) + ",\"metadata\":" + self.metadata + ",\"created_at\":" + _quote(self.created_at) + "}"

struct Homeostat(Copyable, Movable):
    var id: String
    var run_id: String
    var kind: String
    var impulse_id: String
    var status: String
    var values: String
    var metadata: String
    var attempt: Int
    var max_attempts: Int
    var created_at: String
    var updated_at: String

    def __init__(out self, id: String, run_id: String, kind: String,
                 impulse_id: String = "", status: String = "open",
                 values: String = "{}", metadata: String = "{}",
                 attempt: Int = 0, max_attempts: Int = 1,
                 created_at: String = "", updated_at: String = ""):
        self.id = id
        self.run_id = run_id
        self.kind = kind
        self.impulse_id = impulse_id
        self.status = status
        self.values = values
        self.metadata = metadata
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.created_at = created_at
        self.updated_at = updated_at

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id) and _nonempty(self.kind) and _status_homeostat(self.status) and self.attempt >= 0 and self.max_attempts >= 1 and self.attempt <= self.max_attempts

    def validate(self) -> Bool:
        return self.is_valid()

    def can_reopen(self) -> Bool:
        return self.status != "open" and self.attempt < self.max_attempts

    def reopened(self, at: String = "") raises -> Homeostat:
        if self.status == "open": raise Error("homeostat is already open")
        if self.attempt >= self.max_attempts: raise Error("homeostat attempts exhausted")
        var next = self.copy()
        next.status = "open"
        next.attempt += 1
        next.updated_at = at
        return next^

    def to_json(self) -> String:
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"kind\":" + _quote(self.kind) + ",\"impulse_id\":" + _optional_json(self.impulse_id) + ",\"status\":" + _quote(self.status) + ",\"values\":" + self.values + ",\"metadata\":" + self.metadata + ",\"attempt\":" + String(self.attempt) + ",\"max_attempts\":" + String(self.max_attempts) + ",\"created_at\":" + _quote(self.created_at) + ",\"updated_at\":" + _quote(self.updated_at) + "}"


struct Projection(Copyable, Movable):
    var id: String
    var run_id: String
    var name: String
    var version: Int
    var data: String
    var source_event_sequence: Int
    var updated_at: String
    var stale: Bool

    def __init__(out self, id: String, run_id: String, name: String,
                 version: Int = 1, data: String = "{}",
                 source_event_sequence: Int = 0, updated_at: String = "",
                 stale: Bool = False):
        self.id = id
        self.run_id = run_id
        self.name = name
        self.version = version
        self.data = data
        self.source_event_sequence = source_event_sequence
        self.updated_at = updated_at
        self.stale = stale

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id) and _nonempty(self.name) and self.version >= 1 and self.source_event_sequence >= 0

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        var stale_text = "false"
        if self.stale: stale_text = "true"
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"name\":" + _quote(self.name) + ",\"version\":" + String(self.version) + ",\"data\":" + self.data + ",\"source_event_sequence\":" + String(self.source_event_sequence) + ",\"updated_at\":" + _quote(self.updated_at) + ",\"stale\":" + stale_text + "}"


struct RuntimeRef(Copyable, Movable):
    var id: String
    var uri: String
    var metadata: String

    def __init__(out self, id: String, uri: String = "", metadata: String = "{}"):
        self.id = id
        self.uri = uri
        self.metadata = metadata

    def is_valid(self) -> Bool:
        return _nonempty(self.id)

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"id\":" + _quote(self.id) + ",\"uri\":" + _quote(self.uri) + ",\"metadata\":" + self.metadata + "}"


struct RunRef(Copyable, Movable):
    var runtime: RuntimeRef
    var run_id: String

    def __init__(out self, var runtime: RuntimeRef, run_id: String):
        self.runtime = runtime^
        self.run_id = run_id

    def is_valid(self) -> Bool:
        return self.runtime.is_valid() and _nonempty(self.run_id)

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"runtime\":" + self.runtime.to_json() + ",\"run_id\":" + _quote(self.run_id) + "}"


struct EventRef(Copyable, Movable):
    var runtime: RuntimeRef
    var run_id: String
    var event_id: String
    var sequence: Int

    def __init__(out self, var runtime: RuntimeRef, run_id: String,
                 event_id: String = "", sequence: Int = 0):
        self.runtime = runtime^
        self.run_id = run_id
        self.event_id = event_id
        self.sequence = sequence

    def is_valid(self) -> Bool:
        return self.runtime.is_valid() and _nonempty(self.run_id) and self.sequence >= 0

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        return "{\"runtime\":" + self.runtime.to_json() + ",\"run_id\":" + _quote(self.run_id) + ",\"event_id\":" + _quote(self.event_id) + ",\"sequence\":" + String(self.sequence) + "}"


struct RuntimeBudget(Copyable, Movable):
    """Presence-aware delegation limits; absent limits are unlimited."""
    var runtime_hops: Int
    var spawned_runs: Int
    var impulse_count: Int
    var wall_time_seconds: Int
    var attempts: Int
    var reaction_bytes: Int
    var runtime_hops_limited: Bool
    var spawned_runs_limited: Bool
    var impulse_count_limited: Bool
    var wall_time_seconds_limited: Bool
    var attempts_limited: Bool
    var reaction_bytes_limited: Bool

    def __init__(out self, runtime_hops: Int = 0, spawned_runs: Int = 0,
                 impulse_count: Int = 0, wall_time_seconds: Int = 0,
                 attempts: Int = 0, reaction_bytes: Int = 0,
                 runtime_hops_limited: Bool = False,
                 spawned_runs_limited: Bool = False,
                 impulse_count_limited: Bool = False,
                 wall_time_seconds_limited: Bool = False,
                 attempts_limited: Bool = False,
                 reaction_bytes_limited: Bool = False):
        self.runtime_hops = runtime_hops
        self.spawned_runs = spawned_runs
        self.impulse_count = impulse_count
        self.wall_time_seconds = wall_time_seconds
        self.attempts = attempts
        self.reaction_bytes = reaction_bytes
        self.runtime_hops_limited = runtime_hops_limited or runtime_hops != 0
        self.spawned_runs_limited = spawned_runs_limited or spawned_runs != 0
        self.impulse_count_limited = impulse_count_limited or impulse_count != 0
        self.wall_time_seconds_limited = wall_time_seconds_limited or wall_time_seconds != 0
        self.attempts_limited = attempts_limited or attempts != 0
        self.reaction_bytes_limited = reaction_bytes_limited or reaction_bytes != 0

    def is_valid(self) -> Bool:
        return self.runtime_hops >= 0 and self.spawned_runs >= 0 and self.impulse_count >= 0 and self.wall_time_seconds >= 0 and self.attempts >= 0 and self.reaction_bytes >= 0

    def validate(self) -> Bool:
        return self.is_valid()

    def allows(self, runtime_hops: Int = 0, spawned_runs: Int = 0, impulse_count: Int = 0, wall_time_seconds: Int = 0, attempts: Int = 0, reaction_bytes: Int = 0) -> Bool:
        return (not self.runtime_hops_limited or runtime_hops <= self.runtime_hops) and (not self.spawned_runs_limited or spawned_runs <= self.spawned_runs) and (not self.impulse_count_limited or impulse_count <= self.impulse_count) and (not self.wall_time_seconds_limited or wall_time_seconds <= self.wall_time_seconds) and (not self.attempts_limited or attempts <= self.attempts) and (not self.reaction_bytes_limited or reaction_bytes <= self.reaction_bytes)

    def consume(self, runtime_hops: Int = 0, spawned_runs: Int = 0, impulse_count: Int = 0, wall_time_seconds: Int = 0, attempts: Int = 0, reaction_bytes: Int = 0) raises -> RuntimeBudget:
        if runtime_hops < 0 or spawned_runs < 0 or impulse_count < 0 or wall_time_seconds < 0 or attempts < 0 or reaction_bytes < 0 or not self.allows(runtime_hops, spawned_runs, impulse_count, wall_time_seconds, attempts, reaction_bytes):
            raise Error("runtime budget exhausted")
        var next = self.copy()
        if next.runtime_hops_limited: next.runtime_hops -= runtime_hops
        if next.spawned_runs_limited: next.spawned_runs -= spawned_runs
        if next.impulse_count_limited: next.impulse_count -= impulse_count
        if next.wall_time_seconds_limited: next.wall_time_seconds -= wall_time_seconds
        if next.attempts_limited: next.attempts -= attempts
        if next.reaction_bytes_limited: next.reaction_bytes -= reaction_bytes
        return next^

    def to_json(self) -> String:
        return "{\"runtime_hops\":" + (String(self.runtime_hops) if self.runtime_hops_limited else "null") + ",\"spawned_runs\":" + (String(self.spawned_runs) if self.spawned_runs_limited else "null") + ",\"impulse_count\":" + (String(self.impulse_count) if self.impulse_count_limited else "null") + ",\"wall_time_seconds\":" + (String(self.wall_time_seconds) if self.wall_time_seconds_limited else "null") + ",\"attempts\":" + (String(self.attempts) if self.attempts_limited else "null") + ",\"reaction_bytes\":" + (String(self.reaction_bytes) if self.reaction_bytes_limited else "null") + "}"


struct BridgeDelivery(Copyable, Movable):
    var id: String
    var run_id: String
    var idempotency_key: String
    var source: RunRef
    var target: RunRef
    var impulse: Impulse
    var event_ref: EventRef
    var pool_id: String
    var budget: RuntimeBudget
    var status: String
    var attempts: Int
    var metadata: String
    var created_at: String
    var updated_at: String

    def __init__(out self, id: String, run_id: String, idempotency_key: String,
                 var source: RunRef, var target: RunRef, var impulse: Impulse,
                 var event_ref: EventRef = EventRef(RuntimeRef("runtime"), "run"),
                 pool_id: String = "", var budget: RuntimeBudget = RuntimeBudget(),
                 status: String = "pending", attempts: Int = 0,
                 metadata: String = "{}", created_at: String = "",
                 updated_at: String = ""):
        self.id = id
        self.run_id = run_id
        self.idempotency_key = idempotency_key
        self.source = source^
        self.target = target^
        self.impulse = impulse^
        self.event_ref = event_ref^
        self.pool_id = pool_id
        self.budget = budget^
        self.status = status
        self.attempts = attempts
        self.metadata = metadata
        self.created_at = created_at
        self.updated_at = updated_at

    def is_valid(self) -> Bool:
        return _nonempty(self.id) and _nonempty(self.run_id) and _nonempty(self.idempotency_key) and self.source.is_valid() and self.target.is_valid() and self.impulse.is_valid() and self.budget.is_valid() and _status_bridge(self.status) and self.attempts >= 0

    def validate(self) -> Bool:
        return self.is_valid()

    def to_json(self) -> String:
        var event_ref_json = self.event_ref.to_json()
        if self.event_ref.runtime.id == "runtime" and self.event_ref.run_id == "run" and self.event_ref.event_id == "" and self.event_ref.sequence == 0:
            event_ref_json = "null"
        return "{\"id\":" + _quote(self.id) + ",\"run_id\":" + _quote(self.run_id) + ",\"idempotency_key\":" + _quote(self.idempotency_key) + ",\"source\":" + self.source.to_json() + ",\"target\":" + self.target.to_json() + ",\"impulse\":" + self.impulse.to_json() + ",\"event_ref\":" + event_ref_json + ",\"pool_id\":" + _quote(self.pool_id) + ",\"budget\":" + self.budget.to_json() + ",\"status\":" + _quote(self.status) + ",\"attempts\":" + String(self.attempts) + ",\"metadata\":" + self.metadata + ",\"created_at\":" + _quote(self.created_at) + ",\"updated_at\":" + _quote(self.updated_at) + "}"
