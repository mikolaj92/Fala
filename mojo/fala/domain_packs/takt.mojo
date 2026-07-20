"""Takt cascade domain pack — pure mapping onto Fala core records.

Core Fala stays domain-agnostic. This module only builds Impulse / Association /
Homeostat / Projection values with Takt vocabulary.

The cascade engine (hierarchical tact / fusion / actuation) lives in the
separate **Takt** product (v0.2+, exclusive Mojo). Fala hosts it as a
subprocess effector; see `examples/takt-integration/` and Takt
`docs/FALA_INTEGRATION.md`.
"""

from emberjson import Value, to_string
from fala.domain import Association, Homeostat, Impulse, Projection
from fala.json import canonical_json_text


comptime TAKT_DOMAIN_PACK_ID = "takt"
comptime TAKT_CASCADE_REQUEST = "takt.cascade_request"
comptime TAKT_PLANT_LAYER = "takt.plant_layer"
comptime TAKT_ERROR_SIGNAL = "takt.error_signal"
comptime TAKT_SAFETY_INTERLOCK = "takt.safety_interlock"
comptime TAKT_ACTUATION = "takt.actuation"


def _quote(value: String) -> String:
    var result = "\""
    for i in range(value.byte_length()):
        var ch = value[byte=i]
        if ch == "\\":
            result += "\\\\"
        elif ch == "\"":
            result += "\\\""
        elif ch == "\n":
            result += "\\n"
        elif ch == "\r":
            result += "\\r"
        elif ch == "\t":
            result += "\\t"
        else:
            result += String(ch)
    result += "\""
    return result


def _json_string(obj: Value, key: String, default: String = "") raises -> String:
    if not obj.is_object() or key not in obj.object():
        return default
    var item = obj.object()[key].copy()
    if item.is_string():
        return item.string()
    if item.is_null():
        return default
    if item.is_int():
        return String(item.int())
    if item.is_float():
        return String(item.float())
    return default


def _json_optional_number(obj: Value, key: String) raises -> String:
    if not obj.is_object() or key not in obj.object():
        return "null"
    var item = obj.object()[key].copy()
    if item.is_null():
        return "null"
    if item.is_int():
        return String(item.int())
    if item.is_uint():
        return String(item.uint())
    if item.is_float():
        return String(item.float())
    if item.is_string():
        var s = item.string()
        if s == "":
            return "null"
        return s
    return "null"


struct TaktCascadeRequest(Copyable, Movable):
    """Cascade evaluate/run request mapped to impulse type takt.cascade_request.

    Mirrors the host JSON boundary used by `tools/takt_step.sh` (plant + layers).
    """

    var id: String
    var mode: String  # evaluate | run
    var plant_nodes_json: String  # JSON array
    var layers_json: String  # JSON array of layer profiles
    var raw_signals_json: String  # optional JSON array
    var steps: Int  # multi-tact run; 0 = evaluate default
    var metadata_json: String

    def __init__(
        out self,
        id: String,
        mode: String = "evaluate",
        plant_nodes_json: String = "[]",
        layers_json: String = "[]",
        raw_signals_json: String = "[]",
        steps: Int = 0,
        metadata_json: String = "{}",
    ):
        self.id = id
        self.mode = mode
        self.plant_nodes_json = plant_nodes_json
        self.layers_json = layers_json
        self.raw_signals_json = raw_signals_json
        self.steps = steps
        self.metadata_json = metadata_json

    def __init__(out self, *, copy: Self):
        self.id = copy.id
        self.mode = copy.mode
        self.plant_nodes_json = copy.plant_nodes_json
        self.layers_json = copy.layers_json
        self.raw_signals_json = copy.raw_signals_json
        self.steps = copy.steps
        self.metadata_json = copy.metadata_json

    def is_valid(self) -> Bool:
        return (
            self.id.byte_length() > 0
            and (self.mode == "evaluate" or self.mode == "run")
            and self.steps >= 0
        )

    def payload_json(self) raises -> String:
        var raw = (
            "{\"mode\":"
            + _quote(self.mode)
            + ",\"plant_nodes\":"
            + self.plant_nodes_json
            + ",\"layers\":"
            + self.layers_json
            + ",\"raw_signals\":"
            + self.raw_signals_json
            + ",\"steps\":"
            + String(self.steps)
            + "}"
        )
        try:
            return canonical_json_text(raw)
        except err:
            return raw

    def metadata_with_pack(self) raises -> String:
        var base = self.metadata_json
        if base == "" or base == "{}":
            return "{\"domain_pack\":" + _quote(String(TAKT_DOMAIN_PACK_ID)) + "}"
        var parsed = Value(parse_string=base)
        if not parsed.is_object():
            raise Error("takt cascade metadata must be a JSON object")
        var obj = parsed.object().copy()
        obj["domain_pack"] = Value(String(TAKT_DOMAIN_PACK_ID))
        return canonical_json_text(to_string(Value(obj^)))


def impulse_from_cascade(
    request: TaktCascadeRequest,
    run_id: String,
    created_at: String = "",
) raises -> Impulse:
    if not request.is_valid():
        raise Error("takt: cascade requires id and mode evaluate|run")
    if run_id == "":
        raise Error("takt: run_id must not be empty")
    return Impulse(
        id=request.id,
        run_id=run_id,
        impulse_type=String(TAKT_CASCADE_REQUEST),
        payload=request.payload_json(),
        metadata=request.metadata_with_pack(),
        created_at=created_at,
        updated_at=created_at,
    )


def cascade_from_impulse(impulse: Impulse) raises -> TaktCascadeRequest:
    if impulse.impulse_type != String(TAKT_CASCADE_REQUEST):
        raise Error(
            "takt: impulse "
            + impulse.id
            + " is not "
            + String(TAKT_CASCADE_REQUEST)
        )
    var payload = Value(parse_string=impulse.payload)
    if not payload.is_object():
        raise Error("takt: cascade payload must be a JSON object")
    var mode = _json_string(payload, "mode", "evaluate")
    var plant_nodes_json = "[]"
    if "plant_nodes" in payload.object():
        plant_nodes_json = to_string(payload.object()["plant_nodes"].copy())
    var layers_json = "[]"
    if "layers" in payload.object():
        layers_json = to_string(payload.object()["layers"].copy())
    var raw_signals_json = "[]"
    if "raw_signals" in payload.object():
        raw_signals_json = to_string(payload.object()["raw_signals"].copy())
    var steps = 0
    if "steps" in payload.object():
        var steps_item = payload.object()["steps"].copy()
        if steps_item.is_int():
            steps = Int(steps_item.int())
        elif steps_item.is_uint():
            steps = Int(steps_item.uint())
    var metadata_json = impulse.metadata
    try:
        var meta = Value(parse_string=impulse.metadata)
        if meta.is_object() and "domain_pack" in meta.object():
            var m = meta.object().copy()
            var rebuilt = "{"
            var first = True
            for pair in m.items():
                if pair.key == "domain_pack":
                    continue
                if not first:
                    rebuilt += ","
                rebuilt += _quote(pair.key) + ":" + to_string(pair.value.copy())
                first = False
            rebuilt += "}"
            metadata_json = rebuilt
    except err:
        pass
    return TaktCascadeRequest(
        id=impulse.id,
        mode=mode,
        plant_nodes_json=plant_nodes_json,
        layers_json=layers_json,
        raw_signals_json=raw_signals_json,
        steps=steps,
        metadata_json=metadata_json,
    )


def plant_layer_association(
    impulse: Impulse,
    layer: Int,
    tolerance: Float64,
    min_confidence: Float64 = 0.0,
    entropy_threshold: Float64 = 0.0,
    association_id: String = "",
    created_at: String = "",
) raises -> Association:
    """Record one layer profile (ProfilHomeostatyczny surface) as association."""
    _ = cascade_from_impulse(impulse)
    var id = association_id
    if id == "":
        id = "takt.plant_layer:" + impulse.id + ":" + String(layer)
    var values = (
        "{\"layer\":"
        + String(layer)
        + ",\"tolerance\":"
        + String(tolerance)
        + ",\"min_confidence\":"
        + String(min_confidence)
        + ",\"entropy_threshold\":"
        + String(entropy_threshold)
        + "}"
    )
    try:
        values = canonical_json_text(values)
    except err:
        pass
    return Association(
        id=id,
        run_id=impulse.run_id,
        kind=String(TAKT_PLANT_LAYER),
        impulse_id=impulse.id,
        values=values,
        metadata="{\"domain_pack\":" + _quote(String(TAKT_DOMAIN_PACK_ID)) + "}",
        created_at=created_at,
    )


def error_signal_association(
    impulse: Impulse,
    node_id: String,
    aberration: Float64,
    confidence: Float64,
    residual_entropy: Float64,
    reducer: String = "",
    association_id: String = "",
    created_at: String = "",
) raises -> Association:
    """Register a fused ErrorSignal (telemetry footprint) as association."""
    _ = cascade_from_impulse(impulse)
    var id = association_id
    if id == "":
        id = "takt.error_signal:" + impulse.id + ":" + node_id
    var reducer_json = "null"
    if reducer != "":
        reducer_json = _quote(reducer)
    var values = (
        "{\"node_id\":"
        + _quote(node_id)
        + ",\"aberration\":"
        + String(aberration)
        + ",\"confidence\":"
        + String(confidence)
        + ",\"residual_entropy\":"
        + String(residual_entropy)
        + ",\"reducer\":"
        + reducer_json
        + "}"
    )
    try:
        values = canonical_json_text(values)
    except err:
        pass
    return Association(
        id=id,
        run_id=impulse.run_id,
        kind=String(TAKT_ERROR_SIGNAL),
        impulse_id=impulse.id,
        values=values,
        metadata="{\"domain_pack\":" + _quote(String(TAKT_DOMAIN_PACK_ID)) + "}",
        created_at=created_at,
    )


def safety_interlock_homeostat(
    impulse: Impulse,
    node_id: String,
    reason: String = "",
    status: String = "open",
    created_at: String = "",
    max_attempts: Int = 1,
) raises -> Homeostat:
    """Open a defensive wait when cascade fails closed (SafetyInterlock)."""
    _ = cascade_from_impulse(impulse)
    var reason_json = "null"
    if reason != "":
        reason_json = _quote(reason)
    var values = (
        "{\"node_id\":"
        + _quote(node_id)
        + ",\"reason\":"
        + reason_json
        + "}"
    )
    try:
        values = canonical_json_text(values)
    except err:
        pass
    return Homeostat(
        id="takt.interlock:" + impulse.id + ":" + node_id,
        run_id=impulse.run_id,
        kind=String(TAKT_SAFETY_INTERLOCK),
        impulse_id=impulse.id,
        status=status,
        values=values,
        metadata="{\"domain_pack\":" + _quote(String(TAKT_DOMAIN_PACK_ID)) + "}",
        attempt=0,
        max_attempts=max_attempts,
        created_at=created_at,
        updated_at=created_at,
    )


def cascade_projection(
    impulse: Impulse,
    outcome: String = "",
    node_id: String = "",
    projection_id: String = "",
    updated_at: String = "",
) raises -> Projection:
    """Materialize a thin cascade outcome summary for operators."""
    var request = cascade_from_impulse(impulse)
    var name = "takt.cascade:" + impulse.id
    var id = projection_id
    if id == "":
        id = name
    var outcome_json = "null"
    if outcome != "":
        outcome_json = _quote(outcome)
    var node_json = "null"
    if node_id != "":
        node_json = _quote(node_id)
    var data = (
        "{\"impulse_id\":"
        + _quote(impulse.id)
        + ",\"mode\":"
        + _quote(request.mode)
        + ",\"steps\":"
        + String(request.steps)
        + ",\"outcome\":"
        + outcome_json
        + ",\"node_id\":"
        + node_json
        + "}"
    )
    try:
        data = canonical_json_text(data)
    except err:
        pass
    return Projection(
        id=id,
        run_id=impulse.run_id,
        name=name,
        version=1,
        data=data,
        source_event_sequence=0,
        updated_at=updated_at,
        stale=False,
    )


def process_semantics_json() -> String:
    return (
        "{\"plant\":\"host builds hierarchical plant_nodes outside Takt\","
        + "\"cascade\":\"evaluate/run one or more tacts under layer profiles\","
        + "\"fusion\":\"ErrorSignal associations from residual entropy / confidence\","
        + "\"interlock\":\"open safety interlock homeostat when fail-closed\","
        + "\"actuation\":\"reaction footprint for correct_aberration (host applies)\","
        + "\"projection\":\"maintain takt.cascade:{id} summary for operators\"}"
    )
