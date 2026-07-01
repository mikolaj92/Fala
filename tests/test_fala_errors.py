from __future__ import annotations

import unittest

from fala import (
    ContractLintError,
    EmbeddedRuntimeConfigError,
    FalaAdapterError,
    FalaConfigurationError,
    FalaHumanRequired,
    FalaPolicyBlocked,
    FalaRetryableStepError,
    FalaRuntimeError,
    FalaValidationError,
    PipelineRegistryError,
    PipelineRunError,
    ProcessAdapterError,
    RuntimeAuthError,
    RuntimeServiceConcurrencyError,
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

    def test_existing_errors_use_canonical_taxonomy(self) -> None:
        self.assertIsInstance(ProcessAdapterError("adapter"), FalaAdapterError)
        self.assertIsInstance(PipelineRegistryError("registry"), FalaConfigurationError)
        self.assertIsInstance(PipelineRunError("run"), FalaRuntimeError)
        self.assertIsInstance(ContractLintError("lint"), FalaValidationError)
        self.assertIsInstance(
            EmbeddedRuntimeConfigError("config"),
            FalaConfigurationError,
        )
        self.assertIsInstance(
            RuntimeServiceConcurrencyError("busy"),
            FalaRuntimeError,
        )
        self.assertIsInstance(RuntimeAuthError(403, "forbidden"), FalaPolicyBlocked)

    def test_embedded_config_error_keeps_value_error_compatibility(self) -> None:
        self.assertIsInstance(EmbeddedRuntimeConfigError("bad path"), ValueError)


if __name__ == "__main__":
    unittest.main()
