"""Contract-violation addressing for the parent–child boundary.

`fep.*` and adapter codes stay the cause taxonomy. This module only names
blame and the durable payload. Persistence is `violation.record` /
`violation.recorded` on the existing journal tables — no schema bump.
"""

from .adapters import AdapterError
from .json import quote_json_string as quote


def is_contract_violation(error: AdapterError) -> Bool:
    return error.code == "adapter_invalid_result" or error.code == "adapter_missing_output" or error.code == "usage_invalid" or error.code == "output_schema_invalid"


def _cause_code(error: AdapterError) -> String:
    var message = error.message
    var start = message.find("fep.")
    if start < 0:
        return error.code
    var finish = start
    while finish < message.byte_length():
        var ch = String(message[byte=finish])
        if ch == " " or ch == ":" or ch == "\n" or ch == "\t":
            break
        finish += 1
    return String(message[byte=start:finish])


def _expected(error: AdapterError) -> String:
    if error.code == "adapter_missing_output":
        return "result.json"
    if error.code == "usage_invalid":
        return "usage"
    if error.code == "output_schema_invalid":
        return "output_schema"
    return "fala result"


def violation_payload(process_id: String, impulse_id: String, error: AdapterError, blame: String = "effector") raises -> String:
    if blame != "effector" and blame != "correlator":
        raise Error("violation: blame must be effector or correlator")
    if not is_contract_violation(error):
        raise Error("violation: adapter error is not a contract violation")
    return (
        "{\"blame\":"
        + quote(blame)
        + ",\"code\":"
        + quote(_cause_code(error))
        + ",\"effector\":"
        + quote(process_id)
        + ",\"expected\":"
        + quote(_expected(error))
        + ",\"impulse_id\":"
        + quote(impulse_id)
        + ",\"process_id\":"
        + quote(process_id)
        + ",\"received\":"
        + quote(error.message)
        + "}"
    )
