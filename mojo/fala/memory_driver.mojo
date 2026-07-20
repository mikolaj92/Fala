"""Claim → execute → complete loop over MemoryRuntime (core, no SQLite).

Supports:
- static JSON outputs per effector_id (tests)
- NativeFunctionRegistry by effector_id → ref
"""

from std.collections import Dict
from std.collections import List

from fala.adapters import (
    AdapterSpec,
    EffectorRequest,
    NativeFunctionRegistry,
    execute_native_function,
)
from fala.correlation import CorrelationPathSpec
from fala.memory_runtime import MemoryRuntime


struct MemoryDriver(Movable):
    """Drive processes until idle or max ticks."""

    var runtime: MemoryRuntime
    var handlers: Dict[String, String]  # effector_id -> static output JSON
    var refs: Dict[String, String]  # effector_id -> native_function ref
    var registry: NativeFunctionRegistry
    var use_registry: Bool

    def __init__(out self, stream_id: String = "memory://driver"):
        self.runtime = MemoryRuntime(stream_id)
        self.handlers = Dict[String, String]()
        self.refs = Dict[String, String]()
        self.registry = NativeFunctionRegistry()
        self.use_registry = False

    def register_output(mut self, effector_id: String, output_json: String):
        """Map effector_id to fixed JSON output (smoke helpers)."""
        self.handlers[effector_id] = output_json

    def bind_registry(mut self, registry: NativeFunctionRegistry):
        self.registry = registry.copy()
        self.use_registry = True

    def register_ref(mut self, effector_id: String, native_ref: String):
        """Map effector_id to a native_function registry ref."""
        self.refs[effector_id] = native_ref
        self.use_registry = True

    def _execute(
        mut self, effector_id: String, input_json: String, config_json: String
    ) raises -> String:
        if self.use_registry and effector_id in self.refs:
            var native_ref = self.refs[effector_id]
            var request = EffectorRequest(
                "proc",
                AdapterSpec.native_function(native_ref),
                "",
                input_json,
                config_json,
                "",
            )
            var result = execute_native_function(request, self.registry)
            if not result.success:
                raise Error(
                    "native_function failed for "
                    + effector_id
                    + ": "
                    + result.error.message
                )
            return result.output_json
        if effector_id in self.handlers:
            return self.handlers[effector_id]
        return "{\"ok\":true}"

    def drive_until_idle(
        mut self,
        path: CorrelationPathSpec,
        run_id: String,
        worker_id: String = "driver",
        max_ticks: Int = 32,
        lease_seconds: Float64 = 60.0,
    ) raises -> Int:
        var ticks = 0
        while ticks < max_ticks:
            ticks += 1
            self.runtime.clock += 1.0
            var claim = self.runtime.claim_next(worker_id, run_id, lease_seconds)
            if claim.process_id == "":
                var advanced = self.runtime.advance(path, run_id)
                if len(advanced.readied) == 0 and len(advanced.cancelled) == 0:
                    return ticks
                continue
            var process_id = claim.process_id
            var effector_id = ""
            var input_json = "{}"
            var config_json = "{}"
            if process_id in self.runtime.extras:
                var extra = self.runtime.extras[process_id].copy()
                effector_id = extra.effector_id
                input_json = extra.input_json
                config_json = extra.config_json
            var output_json = self._execute(effector_id, input_json, config_json)
            self.runtime.complete_process(process_id, worker_id, output_json)
            _ = self.runtime.advance(path, run_id)
        raise Error("drive_until_idle exceeded max_ticks")
