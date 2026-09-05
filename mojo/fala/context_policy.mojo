"""Vendor-neutral executor context continuity contract."""

from fala.reactions import sha256_bytes
from fala.json import canonical_json_text, quote_json_string as quote


@fieldwise_init
struct ResolvedContext(Copyable, Movable):
    var policy: String
    var key: String
    var source_process_id: String
    var invalidation_digest: String

    def to_json(self) raises -> String:
        return canonical_json_text("{\"invalidation_digest\":" + quote(self.invalidation_digest) + ",\"key\":" + quote(self.key) + ",\"policy\":" + quote(self.policy) + ",\"source_process_id\":" + ("null" if self.source_process_id == "" else quote(self.source_process_id)) + "}")


def resolve_context(policy: String, run_id: String, process_id: String, impulse_id: String, invalidation_digest: String = "", source_process_id: String = "", source_status: String = "", source_provenance_json: String = "") raises -> ResolvedContext:
    if policy == "": return ResolvedContext(policy="", key="", source_process_id="", invalidation_digest="")
    if policy != "fresh" and policy != "resume" and policy != "inherit": raise Error("context.invalid: policy must be fresh, resume, or inherit")
    if run_id == "" or process_id == "": raise Error("context.invalid: run and process identity are required")
    if policy == "inherit":
        if source_process_id == "": raise Error("context.invalid: inherit requires source_process")
        if source_status != "succeeded": raise Error("context.source_not_terminal: inherit source must be succeeded")
        if source_provenance_json == "" or source_provenance_json == "{}": raise Error("context.source_provenance: inherit source provenance is required")
    elif source_process_id != "": raise Error("context.invalid: source_process is valid only for inherit")
    # Physical attempt is deliberately absent: retries preserve this key.
    var identity = policy + "\n" + run_id + "\n" + process_id + "\n" + impulse_id + "\n" + invalidation_digest
    if policy == "inherit": identity = policy + "\n" + source_process_id + "\n" + invalidation_digest
    return ResolvedContext(policy=policy, key="ctx:sha256:" + sha256_bytes(identity), source_process_id=source_process_id, invalidation_digest=invalidation_digest)
