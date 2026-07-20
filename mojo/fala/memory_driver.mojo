"""Claim → execute → complete loop over MemoryRuntime (core, no SQLite)."""

from std.collections import Dict
from std.collections import List

from fala.correlation import CorrelationPathSpec
from fala.memory_runtime import MemoryRuntime


comptime NativeFn = def(String, String) raises capturing[_] -> String


struct MemoryDriver(Movable):
    """Drive processes until idle or max ticks."""

    var runtime: MemoryRuntime
    var handlers: Dict[String, String]  # effector_id -> static output JSON (smoke)
    # For real callables we use a simple registry of effector_id -> output template.

    def __init__(out self, stream_id: String = "memory://driver"):
        self.runtime = MemoryRuntime(stream_id)
        self.handlers = Dict[String, String]()

    def register_output(mut self, effector_id: String, output_json: String):
        """Smoke registry: map effector_id to fixed JSON output."""
        self.handlers[effector_id] = output_json

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
                # Try advance in case pending deps can ready without a claim.
                var advanced = self.runtime.advance(path, run_id)
                if len(advanced.readied) == 0 and len(advanced.cancelled) == 0:
                    return ticks
                continue
            var process_id = claim.process_id
            var effector_id = ""
            var input_json = "{}"
            if process_id in self.runtime.extras:
                var extra = self.runtime.extras[process_id].copy()
                effector_id = extra.effector_id
                input_json = extra.input_json
            var output_json = "{\"ok\":true}"
            if effector_id in self.handlers:
                output_json = self.handlers[effector_id]
            # Optionally echo conduction for sinks
            if input_json != "{}" and effector_id in self.handlers:
                output_json = self.handlers[effector_id]
            self.runtime.complete_process(process_id, worker_id, output_json)
            _ = self.runtime.advance(path, run_id)
        raise Error("drive_until_idle exceeded max_ticks")
