from __future__ import annotations

import unittest

from fala.yaml_loader import workflow_package_from_mapping


class CarrierPackageSchemaTests(unittest.TestCase):
    def test_carrier_first_package_aliases_load_into_legacy_document_fields(self) -> None:
        package = workflow_package_from_mapping(
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
                "capabilities": [
                    {
                        "id": "normalize",
                        "accepts_carrier_types": ["input_text"],
                        "emits_carrier_types": ["normalized_text"],
                    }
                ],
                "pipelines": ["flows/basic.yaml"],
            }
        )

        self.assertEqual(
            [item.id for item in package.document_types],
            ["input_text", "normalized_text"],
        )
        self.assertEqual(package.document_relations[0].source_document_types, ["input_text"])
        self.assertEqual(package.document_relations[0].target_document_types, ["normalized_text"])
        self.assertEqual(package.capabilities[0].accepts_document_types, ["input_text"])
        self.assertEqual(package.capabilities[0].emits_document_types, ["normalized_text"])

    def test_carrier_and_document_package_keys_cannot_be_mixed(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "cannot define both 'carrier_types' and 'document_types'",
        ):
            workflow_package_from_mapping(
                {
                    "id": "mixed_package",
                    "carrier_types": [{"id": "carrier"}],
                    "document_types": [{"id": "document"}],
                    "pipelines": ["flows/basic.yaml"],
                }
            )

    def test_carrier_and_document_capability_keys_cannot_be_mixed(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "cannot define both 'accepts_carrier_types' and 'accepts_document_types'",
        ):
            workflow_package_from_mapping(
                {
                    "id": "mixed_capability",
                    "carrier_types": [{"id": "input_text"}],
                    "capabilities": [
                        {
                            "id": "normalize",
                            "accepts_carrier_types": ["input_text"],
                            "accepts_document_types": ["input_text"],
                        }
                    ],
                    "pipelines": ["flows/basic.yaml"],
                }
            )


if __name__ == "__main__":
    unittest.main()
