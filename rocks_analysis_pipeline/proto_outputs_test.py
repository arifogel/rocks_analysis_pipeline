"""Tests the proto output definitions."""

import unittest

from rocks_analysis_pipeline.proto_outputs import ARCHIVE_TYPES, OUTPUT_BY_NAME, PROTO_OUTPUTS
from rocks_analysis_pipeline.stage1_state import Step
from rocks_analysis_pipeline.stage1_steps import STEP_FNS


class ProtoOutputsTest(unittest.TestCase):
    def test_every_output_step_has_a_function(self) -> None:
        for output in PROTO_OUTPUTS:
            self.assertIn(output.step, STEP_FNS)

    def test_every_step_has_a_function(self) -> None:
        self.assertEqual(set(STEP_FNS), set(Step))

    def test_names_and_filenames_are_unique(self) -> None:
        self.assertEqual(len(OUTPUT_BY_NAME), len(PROTO_OUTPUTS))
        self.assertEqual(len({output.filename for output in PROTO_OUTPUTS}), len(PROTO_OUTPUTS))

    def test_archive_types_follow_output_order(self) -> None:
        self.assertEqual(ARCHIVE_TYPES, tuple(output.name for output in PROTO_OUTPUTS))

    def test_outputs_are_frozen(self) -> None:
        with self.assertRaises(AttributeError):
            PROTO_OUTPUTS[0].name = "x"  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
