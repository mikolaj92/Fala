"""Pure peer-to-peer contract conduction/readiness smoke — no SQLite, no journal I/O."""

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
    
    # Create path spec with peer conduction
    var path = CorrelationPathSpec("chain", effectors^)
    var plan = instantiate_correlation_path(path, "run_p2p")
    
    if len(plan.processes) != 2:
        raise Error("expected two process plans")
    if plan.processes[0].status != "ready":
        raise Error("root must start ready")
    if plan.processes[1].status != "pending":
        raise Error("leaf must start pending")

    # Upstream failed with failure payload
    var failed_states = List[CorrelationExecutionState]()
    failed_states.append(
        CorrelationExecutionState(
            process_id=plan.processes[0].id,
            effector_id="root",
            status="failed",
            attempt=1,
            max_attempts=1,
            output_json="{\"error\":\"something went wrong\"}",
            input_json="{}",
            reactions_json="[]",
            conduction=List[String](),
        )
    )
    failed_states.append(
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
    var advanced = advance_correlation_states(path, failed_states)
    
    if len(advanced.cancelled) != 0:
        raise Error("leaf should not cancel on failed upstream in peer_to_peer mode")
    if len(advanced.readied) != 1:
        raise Error("leaf should become ready in peer_to_peer mode even when upstream failed")
    if advanced.readied[0].effector_id != "leaf":
        raise Error("readied effector must be leaf")
        
    # Check conducted value
    if len(advanced.conduction) != 1:
        raise Error("expected one conducted value")
    if advanced.conduction[0].output_json != "{\"error\":\"something went wrong\"}":
        raise Error("expected conducted output to match failed upstream's payload")

    # Case: Upstream is completely missing/not found in execution states
    var missing_states = List[CorrelationExecutionState]()
    # Only supply leaf state, omit root
    missing_states.append(
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
    var advanced_missing = advance_correlation_states(path, missing_states)
    if len(advanced_missing.cancelled) != 0:
        raise Error("leaf should not cancel on missing upstream in peer_to_peer mode")
    if len(advanced_missing.readied) != 1:
        raise Error("leaf should become ready in peer_to_peer mode even when upstream is missing")
    if advanced_missing.readied[0].effector_id != "leaf":
        raise Error("readied effector must be leaf")
        
    # Check conducted value
    if len(advanced_missing.conduction) != 1:
        raise Error("expected one conducted value for missing upstream")
    if advanced_missing.conduction[0].output_json.find("upstream_not_found") < 0:
        raise Error("expected conducted output to contain upstream_not_found error: " + advanced_missing.conduction[0].output_json)

    print("peer-to-peer schema and conduction smoke ok")
