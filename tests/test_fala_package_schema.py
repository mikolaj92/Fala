from __future__ import annotations

import unittest

from fala.yaml_loader import (
    fala_package_from_mapping,
)


class ImpulsePackageSchemaTests(unittest.TestCase):
    def test_impulse_first_package_loads_canonical_fields(self) -> None:
        package = fala_package_from_mapping(
            {
                "id": "fala_package",
                "version": "2",
                "impulse_types": [
                    {"id": "input_text", "media_types": ["text/plain"]},
                    {"id": "normalized_text", "media_types": ["text/plain"]},
                ],
                "impulse_relations": [
                    {
                        "id": "normalized_from",
                        "source_impulse_types": ["input_text"],
                        "target_impulse_types": ["normalized_text"],
                    }
                ],
                "association_kinds": [
                    {
                        "id": "text_stats",
                        "value_schema": {
                            "type": "object",
                            "properties": {"characters": {"type": "integer"}},
                        },
                    }
                ],
                "reaction_kinds": [
                    {"id": "normalized_text", "media_types": ["text/plain"]}
                ],
                "capabilities": [
                    {
                        "id": "normalize",
                        "accepts_impulse_types": ["input_text"],
                        "emits_impulse_types": ["normalized_text"],
                        "emits_reaction_kinds": ["normalized_text"],
                        "emits_association_kinds": ["text_stats"],
                    }
                ],
                "correlation_paths": [
                    {
                        "id": "basic",
                        "effectors": [
                            {
                                "id": "normalize",
                                "capability": "normalize",
                                "adapter": {
                                    "kind": "python_function",
                                    "ref": "examples.effectors.normalize_text",
                                },
                            }
                        ],
                    }
                ],
                "runtime": {
                    "backend": {"kind": "sqlite", "path": ".fala/state.sqlite"},
                    "reaction_store": {
                        "kind": "filesystem",
                        "root": ".fala/reactions",
                    },
                },
            }
        )

        self.assertEqual([item.id for item in package.impulse_types], ["input_text", "normalized_text"])
        self.assertEqual(package.impulse_relations[0].source_impulse_types, ["input_text"])
        self.assertEqual(package.association_kinds[0].id, "text_stats")
        self.assertEqual(package.capabilities[0].accepts_impulse_types, ["input_text"])
        self.assertEqual(package.correlation_paths[0].effectors[0].adapter.kind, "python_function")
        self.assertEqual(package.runtime.backend.path, ".fala/state.sqlite")

    def test_impulse_first_package_rejects_document_core_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "document_types"):
            fala_package_from_mapping(
                {
                    "id": "fala_package",
                    "document_types": [{"id": "document"}],
                    "correlation_paths": [
                        {
                            "id": "basic",
                            "effectors": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.effectors.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

    def test_impulse_first_package_validates_references(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "Impulse capability 'normalize' accepts_impulse_types reference unknown id",
        ):
            fala_package_from_mapping(
                {
                    "id": "fala_package",
                    "capabilities": [
                        {
                            "id": "normalize",
                            "accepts_impulse_types": ["missing_type"],
                        }
                    ],
                    "correlation_paths": [
                        {
                            "id": "basic",
                            "effectors": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.effectors.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

    def test_impulse_first_package_rejects_package_id_fallback(self) -> None:
        with self.assertRaisesRegex(ValueError, "id"):
            fala_package_from_mapping(
                {
                    "package": "fala_package",
                    "correlation_paths": [
                        {
                            "id": "basic",
                            "effectors": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.effectors.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

    def test_impulse_first_package_rejects_pipeline_id_fallback(self) -> None:
        with self.assertRaisesRegex(ValueError, "pipeline"):
            fala_package_from_mapping(
                {
                    "id": "fala_package",
                    "correlation_paths": [
                        {
                            "pipeline": "basic",
                            "effectors": [
                                {
                                    "id": "normalize",
                                    "capability": "normalize",
                                    "adapter": {
                                        "kind": "python_function",
                                        "ref": "examples.effectors.normalize_text",
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

if __name__ == "__main__":
    unittest.main()
