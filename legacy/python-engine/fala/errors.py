from __future__ import annotations

from typing import Any


class AutonomousCorrelatorError(RuntimeError):
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


class FalaConfigurationError(AutonomousCorrelatorError):
    code = "fala.configuration_error"


class FalaValidationError(AutonomousCorrelatorError):
    code = "fala.validation_error"


class FalaRetryableEffectorError(AutonomousCorrelatorError):
    code = "fala.retryable_effector_error"
    retryable = True


class FalaPermanentEffectorError(AutonomousCorrelatorError):
    code = "fala.permanent_effector_error"


class FalaExternalDependencyError(AutonomousCorrelatorError):
    code = "fala.external_dependency_error"
    retryable = True


class FalaPolicyBlocked(AutonomousCorrelatorError):
    code = "fala.policy_blocked"


class FalaHumanRequired(AutonomousCorrelatorError):
    code = "fala.human_required"
    human_required = True


class FalaDeadlockDetected(AutonomousCorrelatorError):
    code = "fala.deadlock_detected"


class FalaBudgetExceeded(AutonomousCorrelatorError):
    code = "fala.budget_exceeded"


class FalaAdapterError(AutonomousCorrelatorError):
    code = "fala.adapter_error"


class FalaBackendError(AutonomousCorrelatorError):
    code = "fala.backend_error"


__all__ = [
    "FalaAdapterError",
    "FalaBackendError",
    "FalaBudgetExceeded",
    "FalaConfigurationError",
    "FalaDeadlockDetected",
    "FalaExternalDependencyError",
    "FalaHumanRequired",
    "FalaPermanentEffectorError",
    "FalaPolicyBlocked",
    "FalaRetryableEffectorError",
    "AutonomousCorrelatorError",
    "FalaValidationError",
]
