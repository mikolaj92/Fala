from __future__ import annotations

from typing import Any


class FalaRuntimeError(RuntimeError):
    code = "fala.runtime_error"
    retryable = False
    human_required = False

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "human_required": self.human_required,
            "details": self.details,
        }


class FalaConfigurationError(FalaRuntimeError):
    code = "fala.configuration_error"


class FalaValidationError(FalaRuntimeError):
    code = "fala.validation_error"


class FalaRetryableStepError(FalaRuntimeError):
    code = "fala.retryable_step_error"
    retryable = True


class FalaPermanentStepError(FalaRuntimeError):
    code = "fala.permanent_step_error"


class FalaExternalDependencyError(FalaRuntimeError):
    code = "fala.external_dependency_error"
    retryable = True


class FalaPolicyBlocked(FalaRuntimeError):
    code = "fala.policy_blocked"


class FalaHumanRequired(FalaRuntimeError):
    code = "fala.human_required"
    human_required = True


class FalaDeadlockDetected(FalaRuntimeError):
    code = "fala.deadlock_detected"


class FalaBudgetExceeded(FalaRuntimeError):
    code = "fala.budget_exceeded"


class FalaAdapterError(FalaRuntimeError):
    code = "fala.adapter_error"


class FalaBackendError(FalaRuntimeError):
    code = "fala.backend_error"


__all__ = [
    "FalaAdapterError",
    "FalaBackendError",
    "FalaBudgetExceeded",
    "FalaConfigurationError",
    "FalaDeadlockDetected",
    "FalaExternalDependencyError",
    "FalaHumanRequired",
    "FalaPermanentStepError",
    "FalaPolicyBlocked",
    "FalaRetryableStepError",
    "FalaRuntimeError",
    "FalaValidationError",
]
