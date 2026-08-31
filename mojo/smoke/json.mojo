from fala.json import parse_json, canonical_json_text, quote_json_string
from fala.jsonl_journal import encode_command
from fala.journal_port import CommandRecord
from fala.adapters import AdapterError, EffectorResult, adapter_result_json
from fala.domain import Impulse


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("json smoke: " + msg)


def main() raises:
    var parsed = parse_json("{\"b\":1,\"a\":2}")
    print(canonical_json_text(parsed.serialize()))
    var array = parse_json("[true,null,3.5]")
    print(canonical_json_text(array.serialize()))

    # C0 + quote + backslash must share one spelling across CLI, journal, adapter.
    var sample = "line\n" + "\x01" + "quote\"slash\\"
    var quoted = quote_json_string(sample)
    _check(quoted.find("\\n") >= 0, "newline becomes \\\\n")
    _check(quoted.find("\\u0001") >= 0, "SOH becomes \\\\u0001")
    _check(quoted.find("\\u0000") < 0, "SOH is not mapped to \\\\u0000")
    _check(quoted.find("\\\"") >= 0, "quote is escaped")
    _check(quoted.find("\\\\") >= 0, "backslash is escaped")
    _check(parse_json(quoted).value.string() == sample, "quoted C0 roundtrips")

    var command = encode_command(
        CommandRecord(
            id=sample,
            run_id="run",
            command_type="run.create",
            idempotency_key="k",
            actor="",
            correlation_id="",
            causation_id="",
            payload_json="{}",
            created_at="t",
        )
    )
    _check(command.find("\"id\":" + quoted) >= 0, "journal quotes like json.mojo")

    var adapter_json = adapter_result_json(
        EffectorResult(
            success=False,
            output_json="{}",
            stdout="",
            stderr=sample,
            returncode=1,
            waiting=False,
            homeostat_id="",
            metadata_json="{}",
            error=AdapterError("adapter_failed", sample),
        )
    )
    _check(adapter_json.find("\"stderr\":" + quoted) >= 0, "adapter quotes like json.mojo")

    var impulse = Impulse(
        id=sample,
        run_id="run",
        impulse_type="t",
        payload="{}",
        metadata="{}",
        created_at="t",
        updated_at="t",
    )
    _check(impulse.to_json().find("\"id\":" + quoted) >= 0, "domain quotes like json.mojo")
    print("json string quote C0 ok")
