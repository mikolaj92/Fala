"""Signals domain pack — pure mapping onto Fala core records.

Maps sensor / signal vocabulary to Impulse, Association, Homeostat, and
Projection without embedding signal-processing logic in Fala core.
"""

from emberjson import Value, to_string
from fala.domain import Association, Homeostat, Impulse, Projection
from fala.json import canonical_json_text, quote_json_string as _quote


comptime SIGNALS_DOMAIN_PACK_ID = "signals"
comptime SIGNALS_READING = "signals.reading"
comptime SIGNALS_CHANNEL = "signals.channel"
comptime SIGNALS_THRESHOLD = "signals.threshold_review"


struct SignalReading(Copyable, Movable):
    """One numeric reading on a named channel."""

    var id: String
    var channel: String
    var value: Float64
    var unit: String
    var observed_at: String

    def __init__(
        out self,
        id: String,
        channel: String,
        value: Float64,
        unit: String = "",
        observed_at: String = "",
    ):
        self.id = id
        self.channel = channel
        self.value = value
        self.unit = unit
        self.observed_at = observed_at

    def to_payload_json(self) -> String:
        return (
            "{\"id\":"
            + _quote(self.id)
            + ",\"channel\":"
            + _quote(self.channel)
            + ",\"value\":"
            + String(self.value)
            + ",\"unit\":"
            + _quote(self.unit)
            + ",\"observed_at\":"
            + _quote(self.observed_at)
            + "}"
        )


def impulse_from_reading(
    run_id: String,
    reading: SignalReading,
    created_at: String,
    metadata_json: String = "{}",
) -> Impulse:
    """Map a signal reading to impulse type signals.reading."""
    return Impulse(
        reading.id,
        run_id,
        SIGNALS_READING,
        reading.to_payload_json(),
        metadata_json,
        created_at,
        created_at,
    )


def channel_association(
    run_id: String,
    association_id: String,
    impulse_id: String,
    channel: String,
    created_at: String,
) -> Association:
    """Register the channel name as an association (micro-memory)."""
    var values = "{\"channel\":" + _quote(channel) + "}"
    return Association(
        association_id,
        run_id,
        SIGNALS_CHANNEL,
        impulse_id,
        values,
        "{}",
        created_at,
    )


def threshold_homeostat(
    run_id: String,
    homeostat_id: String,
    impulse_id: String,
    channel: String,
    value: Float64,
    threshold: Float64,
    created_at: String,
    max_attempts: Int = 3,
) -> Homeostat:
    """Open a regulation homeostat when a reading needs threshold review.

    Values JSON carries Essential Variable style fields for auto evaluation.
    """
    var values = (
        "{\"channel\":"
        + _quote(channel)
        + ",\"value\":"
        + String(value)
        + ",\"threshold\":"
        + String(threshold)
        + ",\"essential_variable\":"
        + _quote(channel)
        + "}"
    )
    return Homeostat(
        homeostat_id,
        run_id,
        SIGNALS_THRESHOLD,
        impulse_id,
        "open",
        values,
        "{}",
        0,
        max_attempts,
        created_at,
        created_at,
    )


def evaluate_essential_variable(value: Float64, threshold: Float64, mode: String = "gte") -> Bool:
    """Return true when the essential variable is within the desired band.

    Modes: gte (value >= threshold), lte (value <= threshold).
    """
    if mode == "lte":
        return value <= threshold
    return value >= threshold


def regulation_decision(value: Float64, threshold: Float64, mode: String = "gte") -> String:
    """Map EV evaluation to a homeostat terminal intent: complete | wait."""
    if evaluate_essential_variable(value, threshold, mode):
        return "complete"
    return "wait"


def signal_projection(
    run_id: String,
    name: String,
    channel: String,
    last_value: Float64,
    sequence: Int,
    updated_at: String,
) -> Projection:
    """Materialize a simple channel projection."""
    var data = (
        "{\"channel\":"
        + _quote(channel)
        + ",\"last_value\":"
        + String(last_value)
        + ",\"source_event_sequence\":"
        + String(sequence)
        + "}"
    )
    return Projection(
        name + ":" + run_id,
        run_id,
        name,
        1,
        data,
        sequence,
        updated_at,
        False,
    )
