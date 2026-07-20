"""Native Fala lifecycle status values and transition predicates."""

struct ProcessStatus(Copyable, Movable):
    var value: String

    def __init__(out self, value: String):
        self.value = value

    def __eq__(self, other: Self) -> Bool:
        return self.value == other.value

    def __ne__(self, other: Self) -> Bool:
        return self.value != other.value

    def __str__(self) -> String:
        return self.value

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
    def cancel_requested() -> ProcessStatus:
        return ProcessStatus("cancel_requested")

    @staticmethod
    def cancelled() -> ProcessStatus:
        return ProcessStatus("cancelled")

    @staticmethod
    def timed_out() -> ProcessStatus:
        return ProcessStatus("timed_out")

    def is_terminal(self) -> Bool:
        return (
            self.value == "succeeded"
            or self.value == "failed"
            or self.value == "cancelled"
            or self.value == "timed_out"
        )

    def is_known(self) -> Bool:
        return (
            self.value == "pending"
            or self.value == "ready"
            or self.value == "running"
            or self.value == "waiting"
            or self.value == "retry_wait"
            or self.value == "succeeded"
            or self.value == "failed"
            or self.value == "cancel_requested"
            or self.value == "cancelled"
            or self.value == "timed_out"
        )

struct RunStatus(Copyable, Movable):
    var value: String

    def __init__(out self, value: String):
        self.value = value

    def __eq__(self, other: Self) -> Bool:
        return self.value == other.value

    def __ne__(self, other: Self) -> Bool:
        return self.value != other.value
    def __str__(self) -> String:
        return self.value

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
    def cancel_requested() -> RunStatus:
        return RunStatus("cancel_requested")

    @staticmethod
    def cancelled() -> RunStatus:
        return RunStatus("cancelled")

    @staticmethod
    def timed_out() -> RunStatus:
        return RunStatus("timed_out")

    def is_terminal(self) -> Bool:
        return (
            self.value == "completed"
            or self.value == "failed"
            or self.value == "cancelled"
            or self.value == "timed_out"
        )

    def is_known(self) -> Bool:
        return (
            self.value == "created"
            or self.value == "active"
            or self.value == "waiting"
            or self.value == "completed"
            or self.value == "failed"
            or self.value == "cancel_requested"
            or self.value == "cancelled"
            or self.value == "timed_out"
        )

def can_transition_process(from_status: ProcessStatus, to_status: ProcessStatus) -> Bool:
    if not from_status.is_known() or not to_status.is_known():
        return False
    if from_status.is_terminal():
        return False
    if to_status.value == "cancelled" or to_status.value == "timed_out":
        return True
    if from_status.value == "pending":
        return (
            to_status.value == "ready"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "ready":
        return (
            to_status.value == "running"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "running":
        return (
            to_status.value == "succeeded"
            or to_status.value == "failed"
            or to_status.value == "waiting"
            or to_status.value == "retry_wait"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "waiting":
        return (
            to_status.value == "succeeded"
            or to_status.value == "failed"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "retry_wait":
        return (
            to_status.value == "ready"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "cancel_requested":
        return (
            to_status.value == "cancelled"
            or to_status.value == "failed"
            or to_status.value == "timed_out"
        )
    return False
def can_replay_terminal_process(from_status: ProcessStatus, to_status: ProcessStatus) -> Bool:
    """Terminal process transitions may replay only at the same durable state."""
    return from_status.is_terminal() and to_status.is_terminal() and from_status.value == to_status.value

def can_transition_run(from_status: RunStatus, to_status: RunStatus) -> Bool:
    if not from_status.is_known() or not to_status.is_known():
        return False
    if from_status.is_terminal():
        return False
    if from_status.value == to_status.value:
        return False
    if from_status.value == "created":
        return (
            to_status.value == "active"
            or to_status.value == "waiting"
            or to_status.value == "completed"
            or to_status.value == "failed"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "active":
        return (
            to_status.value == "waiting"
            or to_status.value == "completed"
            or to_status.value == "failed"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "waiting":
        return (
            to_status.value == "active"
            or to_status.value == "completed"
            or to_status.value == "failed"
            or to_status.value == "cancel_requested"
            or to_status.value == "cancelled"
            or to_status.value == "timed_out"
        )
    if from_status.value == "cancel_requested":
        return (
            to_status.value == "cancelled"
            or to_status.value == "failed"
            or to_status.value == "timed_out"
        )
    return False
