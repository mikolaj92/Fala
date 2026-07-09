from __future__ import annotations

import unittest

from fala.errors import (
    FalaAdapterError,
    FalaBackendError,
    FalaBudgetExceeded,
    FalaConfigurationError,
    FalaDeadlockDetected,
    FalaExternalDependencyError,
    FalaHumanRequired,
    FalaPermanentStepError,
    FalaPolicyBlocked,
    FalaRetryableStepError,
    FalaRuntimeError,
    FalaValidationError,
)


class FalaErrorTests(unittest.TestCase):
    def test_errors_serialize_for_events_and_reports(self) -> None:
        error = FalaRetryableStepError(
            "temporary failure",
            details={"process_id": "normalize"},
        )

        self.assertEqual(
            error.to_dict(),
            {
                "code": "fala.retryable_step_error",
                "message": "temporary failure",
                "retryable": True,
                "human_required": False,
                "details": {"process_id": "normalize"},
            },
        )

    def test_error_flags_distinguish_human_and_retry_paths(self) -> None:
        self.assertTrue(FalaRetryableStepError("retry").retryable)
        self.assertTrue(FalaHumanRequired("review").human_required)
        self.assertFalse(FalaRuntimeError("plain").retryable)

    def test_error_taxonomy_is_complete(self) -> None:
        expected = {
            FalaAdapterError: "fala.adapter_error",
            FalaBackendError: "fala.backend_error",
            FalaBudgetExceeded: "fala.budget_exceeded",
            FalaConfigurationError: "fala.configuration_error",
            FalaDeadlockDetected: "fala.deadlock_detected",
            FalaExternalDependencyError: "fala.external_dependency_error",
            FalaHumanRequired: "fala.human_required",
            FalaPermanentStepError: "fala.permanent_step_error",
            FalaPolicyBlocked: "fala.policy_blocked",
            FalaRetryableStepError: "fala.retryable_step_error",
            FalaValidationError: "fala.validation_error",
        }

        for error_class, code in expected.items():
            error = error_class("x")
            self.assertIsInstance(error, FalaRuntimeError)
            self.assertEqual(error.code, code)


if __name__ == "__main__":
    unittest.main()
