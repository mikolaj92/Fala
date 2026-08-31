"""Splot arbitration domain pack — pure mapping onto Fala core records.

Core Fala stays domain-agnostic. This module only builds Impulse / Association /
Homeostat / Projection values with Splot vocabulary.
"""

from emberjson import Value, to_string
from fala.domain import Association, Homeostat, Impulse, Projection
from fala.json import canonical_json_text, quote_json_string as _quote


comptime SPLOT_DOMAIN_PACK_ID = "splot"
comptime SPLOT_ARBITRATION_CASE = "splot.arbitration_case"
comptime SPLOT_JURISDICTION = "splot.jurisdiction"
comptime SPLOT_REVIEW = "splot.review"


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
    """Return JSON number text or null literal for amount fields."""
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


struct SplotArbitrationCase(Copyable, Movable):
    """Arbitration case input mapped to impulse type splot.arbitration_case."""

    var id: String
    var claim_id: String
    var claimant: String
    var respondent: String
    var amount_json: String  # JSON number or null
    var currency: String
    var rules: String
    var values_json: String
    var metadata_json: String
    var reactions_json: String  # JSON array

    def __init__(
        out self,
        id: String,
        claim_id: String,
        claimant: String,
        respondent: String,
        amount_json: String = "null",
        currency: String = "",
        rules: String = "",
        values_json: String = "{}",
        metadata_json: String = "{}",
        reactions_json: String = "[]",
    ):
        self.id = id
        self.claim_id = claim_id
        self.claimant = claimant
        self.respondent = respondent
        self.amount_json = amount_json
        self.currency = currency
        self.rules = rules
        self.values_json = values_json
        self.metadata_json = metadata_json
        self.reactions_json = reactions_json

    def __init__(out self, *, copy: Self):
        self.id = copy.id
        self.claim_id = copy.claim_id
        self.claimant = copy.claimant
        self.respondent = copy.respondent
        self.amount_json = copy.amount_json
        self.currency = copy.currency
        self.rules = copy.rules
        self.values_json = copy.values_json
        self.metadata_json = copy.metadata_json
        self.reactions_json = copy.reactions_json

    def is_valid(self) -> Bool:
        return (
            self.id.byte_length() > 0
            and self.claim_id.byte_length() > 0
            and self.claimant.byte_length() > 0
            and self.respondent.byte_length() > 0
        )

    def payload_json(self) raises -> String:
        var currency_json = "null"
        if self.currency != "":
            currency_json = _quote(self.currency)
        var rules_json = "null"
        if self.rules != "":
            rules_json = _quote(self.rules)
        var raw = (
            "{\"claim_id\":"
            + _quote(self.claim_id)
            + ",\"claimant\":"
            + _quote(self.claimant)
            + ",\"respondent\":"
            + _quote(self.respondent)
            + ",\"amount\":"
            + self.amount_json
            + ",\"currency\":"
            + currency_json
            + ",\"rules\":"
            + rules_json
            + ",\"values\":"
            + self.values_json
            + ",\"reactions\":"
            + self.reactions_json
            + "}"
        )
        try:
            return canonical_json_text(raw)
        except err:
            return raw

    def metadata_with_pack(self) raises -> String:
        var base = self.metadata_json
        if base == "" or base == "{}":
            return "{\"domain_pack\":" + _quote(String(SPLOT_DOMAIN_PACK_ID)) + "}"
        # Merge domain_pack key without full object merge library.
        var parsed = Value(parse_string=base)
        if not parsed.is_object():
            raise Error("splot case metadata must be a JSON object")
        var obj = parsed.object().copy()
        obj["domain_pack"] = Value(String(SPLOT_DOMAIN_PACK_ID))
        return canonical_json_text(to_string(Value(obj^)))


def impulse_from_case(
    arbitration_case: SplotArbitrationCase,
    run_id: String,
    created_at: String = "",
) raises -> Impulse:
    if not arbitration_case.is_valid():
        raise Error("splot: case requires id, claim_id, claimant, respondent")
    if run_id == "":
        raise Error("splot: run_id must not be empty")
    return Impulse(
        id=arbitration_case.id,
        run_id=run_id,
        impulse_type=String(SPLOT_ARBITRATION_CASE),
        payload=arbitration_case.payload_json(),
        metadata=arbitration_case.metadata_with_pack(),
        created_at=created_at,
        updated_at=created_at,
    )


def case_from_impulse(impulse: Impulse) raises -> SplotArbitrationCase:
    if impulse.impulse_type != String(SPLOT_ARBITRATION_CASE):
        raise Error(
            "splot: impulse "
            + impulse.id
            + " is not "
            + String(SPLOT_ARBITRATION_CASE)
        )
    var payload = Value(parse_string=impulse.payload)
    if not payload.is_object():
        raise Error("splot: case payload must be a JSON object")
    var claim_id = _json_string(payload, "claim_id")
    var claimant = _json_string(payload, "claimant")
    var respondent = _json_string(payload, "respondent")
    if claim_id == "" or claimant == "" or respondent == "":
        raise Error("splot: claim_id, claimant, respondent are required")
    var amount_json = _json_optional_number(payload, "amount")
    var currency = _json_string(payload, "currency")
    var rules = _json_string(payload, "rules")
    var values_json = "{}"
    if "values" in payload.object():
        values_json = to_string(payload.object()["values"].copy())
    var reactions_json = "[]"
    if "reactions" in payload.object():
        reactions_json = to_string(payload.object()["reactions"].copy())
    var metadata_json = impulse.metadata
    # Strip domain_pack from case metadata surface.
    try:
        var meta = Value(parse_string=impulse.metadata)
        if meta.is_object() and "domain_pack" in meta.object():
            var m = meta.object().copy()
            # Rebuild without domain_pack via re-encode remaining keys.
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
    return SplotArbitrationCase(
        id=impulse.id,
        claim_id=claim_id,
        claimant=claimant,
        respondent=respondent,
        amount_json=amount_json,
        currency=currency,
        rules=rules,
        values_json=values_json,
        metadata_json=metadata_json,
        reactions_json=reactions_json,
    )


def jurisdiction_association(
    impulse: Impulse,
    admissible: Bool,
    reason: String = "",
    association_id: String = "",
    created_at: String = "",
) raises -> Association:
    var arbitration_case = case_from_impulse(impulse)
    var id = association_id
    if id == "":
        id = "splot.jurisdiction:" + arbitration_case.claim_id
    var reason_json = "null"
    if reason != "":
        reason_json = _quote(reason)
    var admissible_json = "false"
    if admissible:
        admissible_json = "true"
    var values = (
        "{\"claim_id\":"
        + _quote(arbitration_case.claim_id)
        + ",\"admissible\":"
        + admissible_json
        + ",\"reason\":"
        + reason_json
        + "}"
    )
    try:
        values = canonical_json_text(values)
    except err:
        pass
    return Association(
        id=id,
        run_id=impulse.run_id,
        kind=String(SPLOT_JURISDICTION),
        impulse_id=impulse.id,
        values=values,
        metadata="{\"domain_pack\":" + _quote(String(SPLOT_DOMAIN_PACK_ID)) + "}",
        created_at=created_at,
    )


def review_homeostat(
    impulse: Impulse,
    status: String = "open",
    created_at: String = "",
) raises -> Homeostat:
    var arbitration_case = case_from_impulse(impulse)
    return Homeostat(
        id="splot_review:" + arbitration_case.claim_id,
        run_id=impulse.run_id,
        kind=String(SPLOT_REVIEW),
        impulse_id=impulse.id,
        status=status,
        values="{\"claim_id\":" + _quote(arbitration_case.claim_id) + "}",
        metadata="{\"domain_pack\":" + _quote(String(SPLOT_DOMAIN_PACK_ID)) + "}",
        attempt=0,
        max_attempts=1,
        created_at=created_at,
        updated_at=created_at,
    )


def case_projection(
    impulse: Impulse,
    projection_id: String = "",
    updated_at: String = "",
) raises -> Projection:
    var arbitration_case = case_from_impulse(impulse)
    var name = "splot.case:" + arbitration_case.claim_id
    var id = projection_id
    if id == "":
        id = name
    var reaction_count = 0
    try:
        var reactions = Value(parse_string=arbitration_case.reactions_json)
        if reactions.is_array():
            reaction_count = len(reactions.array())
    except err:
        reaction_count = 0
    var currency_json = "null"
    if arbitration_case.currency != "":
        currency_json = _quote(arbitration_case.currency)
    var rules_json = "null"
    if arbitration_case.rules != "":
        rules_json = _quote(arbitration_case.rules)
    var data = (
        "{\"impulse_id\":"
        + _quote(impulse.id)
        + ",\"claim_id\":"
        + _quote(arbitration_case.claim_id)
        + ",\"claimant\":"
        + _quote(arbitration_case.claimant)
        + ",\"respondent\":"
        + _quote(arbitration_case.respondent)
        + ",\"amount\":"
        + arbitration_case.amount_json
        + ",\"currency\":"
        + currency_json
        + ",\"rules\":"
        + rules_json
        + ",\"reaction_count\":"
        + String(reaction_count)
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
        "{\"intake\":\"accept arbitration case impulse and source reactions\","
        + "\"jurisdiction\":\"record jurisdiction and admissibility associations\","
        + "\"triage\":\"open or complete human review homeostats\","
        + "\"award_projection\":\"maintain case summary projection for operators\"}"
    )
