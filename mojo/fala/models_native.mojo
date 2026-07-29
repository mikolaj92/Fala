"""Typed, Python-free schema-v6 runtime contracts for Fala.

Dynamic values are intentionally represented as canonical JSON text.  Empty
strings represent nullable text columns; numeric budgets use -1 for unlimited.
"""


struct RunStatus(Copyable, Movable):
    var value: String

    def __init__(out self, value: String):
        self.value = value

    def is_terminal(self) -> Bool:
        return self.value == "completed" or self.value == "failed" or self.value == "cancelled" or self.value == "timed_out"

    @staticmethod
    def created() -> RunStatus:
        return RunStatus("created")

    @staticmethod
    def active() -> RunStatus:
        return RunStatus("active")

    @staticmethod
    def waiting() -> RunStatus:
        return RunStatus("waiting")

    @staticmethod
    def completed() -> RunStatus:
        return RunStatus("completed")

    @staticmethod
    def failed() -> RunStatus:
        return RunStatus("failed")

    @staticmethod
    def cancelled() -> RunStatus:
        return RunStatus("cancelled")

    @staticmethod
    def timed_out() -> RunStatus:
        return RunStatus("timed_out")


struct ProcessStatus(Copyable, Movable):
    var value: String

    def __init__(out self, value: String):
        self.value = value

    def is_pending(self) -> Bool:
        return self.value == "pending"

    def is_ready(self) -> Bool:
        return self.value == "ready"

    def is_running(self) -> Bool:
        return self.value == "running"

    def is_waiting(self) -> Bool:
        return self.value == "waiting"

    def is_retry_wait(self) -> Bool:
        return self.value == "retry_wait"

    def is_succeeded(self) -> Bool:
        return self.value == "succeeded"

    def is_failed(self) -> Bool:
        return self.value == "failed"

    def is_cancel_requested(self) -> Bool:
        return self.value == "cancel_requested"

    def is_cancelled(self) -> Bool:
        return self.value == "cancelled"

    def is_timed_out(self) -> Bool:
        return self.value == "timed_out"

    def is_terminal(self) -> Bool:
        return self.is_succeeded() or self.is_failed() or self.is_cancelled() or self.is_timed_out()

    @staticmethod
    def pending() -> ProcessStatus:
        return ProcessStatus("pending")

    @staticmethod
    def ready() -> ProcessStatus:
        return ProcessStatus("ready")

    @staticmethod
    def running() -> ProcessStatus:
        return ProcessStatus("running")

    @staticmethod
    def waiting() -> ProcessStatus:
        return ProcessStatus("waiting")

    @staticmethod
    def retry_wait() -> ProcessStatus:
        return ProcessStatus("retry_wait")

    @staticmethod
    def succeeded() -> ProcessStatus:
        return ProcessStatus("succeeded")

    @staticmethod
    def failed() -> ProcessStatus:
        return ProcessStatus("failed")

    @staticmethod
    def cancelled() -> ProcessStatus:
        return ProcessStatus("cancelled")

    @staticmethod
    def timed_out() -> ProcessStatus:
        return ProcessStatus("timed_out")


struct HomeostatStatus(Copyable, Movable):
    var value: String

    def __init__(out self, value: String):
        self.value = value

    def is_terminal(self) -> Bool:
        return self.value == "completed" or self.value == "cancelled" or self.value == "expired"

    @staticmethod
    def open() -> HomeostatStatus:
        return HomeostatStatus("open")

    @staticmethod
    def completed() -> HomeostatStatus:
        return HomeostatStatus("completed")

    @staticmethod
    def cancelled() -> HomeostatStatus:
        return HomeostatStatus("cancelled")

    @staticmethod
    def expired() -> HomeostatStatus:
        return HomeostatStatus("expired")


struct Run(Copyable, Movable):
    var id: String
    var status: String
    var title: String
    var package_id: String
    var package_version: String
    var package_digest: String
    var correlation_path_id: String
    var correlation_path_digest: String
    var runtime_version: String
    var backend_version: String
    var schema_version: Int
    var metadata: String
    var created_at: String
    var updated_at: String
    var started_at: String
    var finished_at: String

    def __init__(out self, id: String, status: String, title: String, package_id: String, package_version: String, package_digest: String, correlation_path_id: String, correlation_path_digest: String, runtime_version: String, backend_version: String, schema_version: Int, metadata: String, created_at: String, updated_at: String, started_at: String, finished_at: String):
        self.id = id
        self.status = status
        self.title = title
        self.package_id = package_id
        self.package_version = package_version
        self.package_digest = package_digest
        self.correlation_path_id = correlation_path_id
        self.correlation_path_digest = correlation_path_digest
        self.runtime_version = runtime_version
        self.backend_version = backend_version
        self.schema_version = schema_version
        self.metadata = metadata
        self.created_at = created_at
        self.updated_at = updated_at
        self.started_at = started_at
        self.finished_at = finished_at


struct Impulse(Copyable, Movable):
    var id: String
    var run_id: String
    var impulse_type: String
    var payload: String
    var metadata: String
    var created_at: String
    var updated_at: String

    def __init__(out self, id: String, run_id: String, impulse_type: String, payload: String, metadata: String, created_at: String, updated_at: String):
        self.id = id
        self.run_id = run_id
        self.impulse_type = impulse_type
        self.payload = payload
        self.metadata = metadata
        self.created_at = created_at
        self.updated_at = updated_at


struct ImpulseType(Copyable, Movable):
    var id: String
    var run_id: String
    var title: String
    var description: String
    var media_types: List[String]
    var value_schema: String
    var metadata: String
    var created_at: String
    var updated_at: String

    def __init__(out self, id: String, run_id: String, title: String, description: String, var media_types: List[String], value_schema: String, metadata: String, created_at: String, updated_at: String):
        self.id = id
        self.run_id = run_id
        self.title = title
        self.description = description
        self.media_types = media_types^
        self.value_schema = value_schema
        self.metadata = metadata
        self.created_at = created_at
        self.updated_at = updated_at


struct ImpulseRelation(Copyable, Movable):
    var id: String
    var run_id: String
    var relation_type: String
    var source_impulse_id: String
    var target_impulse_id: String
    var metadata: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, relation_type: String, source_impulse_id: String, target_impulse_id: String, metadata: String, created_at: String):
        self.id = id
        self.run_id = run_id
        self.relation_type = relation_type
        self.source_impulse_id = source_impulse_id
        self.target_impulse_id = target_impulse_id
        self.metadata = metadata
        self.created_at = created_at


struct RuntimeCommand(Copyable, Movable):
    var id: String
    var run_id: String
    var command_type: String
    var idempotency_key: String
    var actor: String
    var correlation_id: String
    var causation_id: String
    var payload: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, command_type: String, idempotency_key: String, actor: String, correlation_id: String, causation_id: String, payload: String, created_at: String):
        self.id = id
        self.run_id = run_id
        self.command_type = command_type
        self.idempotency_key = idempotency_key
        self.actor = actor
        self.correlation_id = correlation_id
        self.causation_id = causation_id
        self.payload = payload
        self.created_at = created_at


struct RuntimeEvent(Copyable, Movable):
    var id: String
    var run_id: String
    var event_type: String
    var schema_version: Int
    var impulse_id: String
    var process_id: String
    var sequence: Int
    var command_id: String
    var actor: String
    var correlation_id: String
    var causation_id: String
    var payload: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, event_type: String, schema_version: Int, impulse_id: String, process_id: String, sequence: Int, command_id: String, actor: String, correlation_id: String, causation_id: String, payload: String, created_at: String):
        self.id = id
        self.run_id = run_id
        self.event_type = event_type
        self.schema_version = schema_version
        self.impulse_id = impulse_id
        self.process_id = process_id
        self.sequence = sequence
        self.command_id = command_id
        self.actor = actor
        self.correlation_id = correlation_id
        self.causation_id = causation_id
        self.payload = payload
        self.created_at = created_at


struct Association(Copyable, Movable):
    var id: String
    var run_id: String
    var kind: String
    var impulse_id: String
    var values: String
    var metadata: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, kind: String, impulse_id: String, values: String, metadata: String, created_at: String):
        self.id = id
        self.run_id = run_id
        self.kind = kind
        self.impulse_id = impulse_id
        self.values = values
        self.metadata = metadata
        self.created_at = created_at


struct Reaction(Copyable, Movable):
    var id: String
    var run_id: String
    var kind: String
    var uri: String
    var impulse_id: String
    var media_type: String
    var size_bytes: Int
    var content_hash: String
    var metadata: String
    var created_at: String

    def __init__(out self, id: String, run_id: String, kind: String, uri: String, impulse_id: String, media_type: String, size_bytes: Int, content_hash: String, metadata: String, created_at: String):
        self.id = id
        self.run_id = run_id
        self.kind = kind
        self.uri = uri
        self.impulse_id = impulse_id
        self.media_type = media_type
        self.size_bytes = size_bytes
        self.content_hash = content_hash
        self.metadata = metadata
        self.created_at = created_at


struct Process(Copyable, Movable):
    var id: String
    var run_id: String
    var process_type: String
    var impulse_id: String
    var status: ProcessStatus
    var priority: Int
    var attempt: Int
    var max_attempts: Int
    var available_at: String
    var lease_owner: String
    var lease_expires_at: String
    var input: String
    var output: String
    var error: String
    var metadata: String
    var created_at: String
    var updated_at: String
    var started_at: String
    var finished_at: String
    var output_schema: String

    def __init__(out self, id: String, run_id: String, process_type: String, impulse_id: String, var status: ProcessStatus, priority: Int, attempt: Int, max_attempts: Int, available_at: String, lease_owner: String, lease_expires_at: String, input: String, output: String, error: String, metadata: String, created_at: String, updated_at: String, started_at: String, finished_at: String, output_schema: String):
        self.id = id
        self.run_id = run_id
        self.process_type = process_type
        self.impulse_id = impulse_id
        self.status = status^
        self.priority = priority
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.available_at = available_at
        self.lease_owner = lease_owner
        self.lease_expires_at = lease_expires_at
        self.input = input
        self.output = output
        self.error = error
        self.metadata = metadata
        self.created_at = created_at
        self.updated_at = updated_at
        self.started_at = started_at
        self.finished_at = finished_at
        self.output_schema = output_schema

    def is_pending(self) -> Bool:
        return self.status.is_pending()

    def is_ready(self) -> Bool:
        return self.status.is_ready()

    def is_running(self) -> Bool:
        return self.status.is_running()

    def is_waiting(self) -> Bool:
        return self.status.is_waiting()

    def is_terminal(self) -> Bool:
        return self.status.is_terminal()


struct Homeostat(Copyable, Movable):
    var id: String
    var run_id: String
    var kind: String
    var impulse_id: String
    var status: String
    var values: String
    var metadata: String
    var attempt: Int
    var max_attempts: Int
    var created_at: String
    var updated_at: String

    def __init__(out self, id: String, run_id: String, kind: String, impulse_id: String, status: String, values: String, metadata: String, attempt: Int, max_attempts: Int, created_at: String, updated_at: String):
        self.id = id
        self.run_id = run_id
        self.kind = kind
        self.impulse_id = impulse_id
        self.status = status
        self.values = values
        self.metadata = metadata
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.created_at = created_at
        self.updated_at = updated_at


struct Projection(Copyable, Movable):
    var id: String
    var run_id: String
    var name: String
    var version: Int
    var data: String
    var source_event_sequence: Int
    var updated_at: String

    def __init__(out self, id: String, run_id: String, name: String, version: Int, data: String, source_event_sequence: Int, updated_at: String):
        self.id = id
        self.run_id = run_id
        self.name = name
        self.version = version
        self.data = data
        self.source_event_sequence = source_event_sequence
        self.updated_at = updated_at


struct RuntimeRef(Copyable, Movable):
    var id: String
    var uri: String
    var metadata: String

    def __init__(out self, id: String, uri: String, metadata: String):
        self.id = id
        self.uri = uri
        self.metadata = metadata


struct RuntimeBudget(Copyable, Movable):
    # A non-negative value is a finite budget; -1 means unlimited.
    var runtime_hops: Int
    var spawned_runs: Int
    var impulse_count: Int
    var wall_time_seconds: Float64
    var attempts: Int
    var reaction_bytes: Int

    def __init__(out self, runtime_hops: Int, spawned_runs: Int, impulse_count: Int, wall_time_seconds: Float64, attempts: Int, reaction_bytes: Int):
        self.runtime_hops = runtime_hops
        self.spawned_runs = spawned_runs
        self.impulse_count = impulse_count
        self.wall_time_seconds = wall_time_seconds
        self.attempts = attempts
        self.reaction_bytes = reaction_bytes

    @staticmethod
    def unlimited() -> RuntimeBudget:
        return RuntimeBudget(-1, -1, -1, -1.0, -1, -1)
