from __future__ import annotations

import unittest

from fala.yaml_loader import (
    carrier_workflow_package_from_mapping,
)


class CarrierPackageSchemaTests(unittest.TestCase):
    def test_carrier_first_package_loads_canonical_fields(self) -> None:
        package = carrier_workflow_package_from_mapping(
            {
                "id": "carrier_package",
                "version": "2",
                "carrier_types": [
                    {"id": "input_text", "media_types": ["text/plain"]},
                    {"id": "normalized_text", "media_types": ["text/plain"]},
                ],
                "carrier_relations": [
                    {
                        "id": "normalized_from",
                        "source_carrier_types": ["input_text"],
                        "target_carrier_types": ["normalized_text"],
                    }
                ],
                "observation_kinds": [
                    {
                        "id": "text_stats",
                        "value_schema": {
                            "type": "object",
                            "properties": {"characters": {"type": "integer"}},
                        },
                    }
                ],
                "artifact_kinds": [
                    {"id": "normalized_text", "media_types": ["text/plain"]}
                ],
                "capabilities": [
                    {
                        "id": "normalize",
                        "accepts_carrier_types": ["input_text"],
                        "emits_carrier_types": ["normalized_text"],
                        "emits_artifact_kinds": ["normalized_text"],
                        "emits_observation_kinds": ["text_stats"],
                    }
                ],
                "flows": [
                    {
                        "id": "basic",
                        "steps": [
                            {
                                "id": "normalize",
                                "capability": "normalize",
                                "adapter": {
                                    "kind": "python_function",
                                    "ref": "examples.steps.normalize_text",
                                },
                            }
                        ],
                    }
                ],
                "runtime": {
                    "backend": {"kind": "sqlite", "path": ".fala/state.sqlite"},
                    "artifact_store": {
                        "kind": "filesystem",
                        "root": ".fala/artifacts",
                    },
                },
            }
        )

        self.assertEqual([item.id for item in package.carrier_types], ["input_text", "normalized_text"])
        self.assertEqual(package.carrier_relations[0].source_carrier_types, ["input_text"])
        self.assertEqual(package.observation_kinds[0].id, "text_stats")
        self.assertEqual(package.capabilities[0].accepts_carrier_types, ["input_text"])
        self.assertEqual(package.flows[0].steps[0].adapter.kind, "python_function")
        self.assertEqual(package.runtime.backend.path, ".fala/state.sqlite")

    def test_carrier_first_package_rejects_document_core_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "document_types"):
            carrier_workflow_package_from_mapping(
                {
                    "id": "carrier_package",
                    "document_types": [{"id": "document"}],
                    "flows": [
                        {
                            "id": "basic",
                            "steps": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.steps.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

    def test_carrier_first_package_validates_references(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "Carrier capability 'normalize' accepts_carrier_types reference unknown id",
        ):
            carrier_workflow_package_from_mapping(
                {
                    "id": "carrier_package",
                    "capabilities": [
                        {
                            "id": "normalize",
                            "accepts_carrier_types": ["missing_type"],
                        }
                    ],
                    "flows": [
                        {
                            "id": "basic",
                            "steps": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.steps.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

    def test_carrier_first_package_rejects_package_id_fallback(self) -> None:
        with self.assertRaisesRegex(ValueError, "id"):
            carrier_workflow_package_from_mapping(
                {
                    "package": "carrier_package",
                    "flows": [
                        {
                            "id": "basic",
                            "steps": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.steps.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

    def test_carrier_first_package_rejects_pipeline_id_fallback(self) -> None:
        with self.assertRaisesRegex(ValueError, "pipeline"):
            carrier_workflow_package_from_mapping(
                {
                    "id": "carrier_package",
                    "flows": [
                        {
                            "pipeline": "basic",
                            "steps": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.steps.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

if __name__ == "__main__":
    unittest.main()
