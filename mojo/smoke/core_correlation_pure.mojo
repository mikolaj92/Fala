"""Pure correlation planning/advance smoke — no SQLite, no journal I/O."""

from std.collections import List

from fala.correlation import (
    CorrelationEffectorSpec,
    CorrelationExecutionState,
    CorrelationPathSpec,
    advance_correlation_states,
    instantiate_correlation_path,
)


def _one(value: String) -> List[String]:
    var values = List[String]()
    values.append(value)
    return values^


def main() raises:
    var root = CorrelationEffectorSpec.create("root", "source")
    var leaf = CorrelationEffectorSpec.create("leaf", "sink", _one("root"))
    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(root^)
    effectors.append(leaf^)
    var path = CorrelationPathSpec("chain", effectors^)
    var plan = instantiate_correlation_path(path, "run_pure")
    if len(plan.processes) != 2:
        raise Error("expected two process plans")
    if plan.processes[0].status != "ready":
        raise Error("root must start ready")
    if plan.processes[1].status != "pending":
        raise Error("leaf must start pending")

    var states = List[CorrelationExecutionState]()
    states.append(
        CorrelationExecutionState(
            process_id=plan.processes[0].id,
            effector_id="root",
            status="succeeded",
            attempt=1,
            max_attempts=1,
            output_json="{\"value\":1}",
            input_json="{}",
            reactions_json="[]",
            conduction=List[String](),
        )
    )
    states.append(
        CorrelationExecutionState(
            process_id=plan.processes[1].id,
            effector_id="leaf",
            status="pending",
            attempt=0,
            max_attempts=1,
            output_json="{}",
            input_json="{}",
            reactions_json="[]",
            conduction=_one("root"),
        )
    )
    var advanced = advance_correlation_states(path, states)
    if len(advanced.readied) != 1:
        raise Error("leaf should become ready after root succeeds")
    if advanced.readied[0].effector_id != "leaf":
        raise Error("readied effector must be leaf")

    # Dead upstream cancels leaf
    var dead_states = List[CorrelationExecutionState]()
    dead_states.append(
        CorrelationExecutionState(
            process_id=plan.processes[0].id,
            effector_id="root",
            status="failed",
            attempt=1,
            max_attempts=1,
            output_json="{}",
            input_json="{}",
            reactions_json="[]",
            conduction=List[String](),
        )
    )
    dead_states.append(
        CorrelationExecutionState(
            process_id=plan.processes[1].id,
            effector_id="leaf",
            status="pending",
            attempt=0,
            max_attempts=1,
            output_json="{}",
            input_json="{}",
            reactions_json="[]",
            conduction=_one("root"),
        )
    )
    var cancelled = advance_correlation_states(path, dead_states)
    if len(cancelled.cancelled) != 1:
        raise Error("leaf should cancel on dead upstream")

    print("core correlation pure smoke ok")
