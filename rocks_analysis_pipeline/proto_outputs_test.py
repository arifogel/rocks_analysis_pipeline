"""Tests the proto output definitions."""

import unittest

from rocks_analysis_pipeline.proto_outputs import ProtoOutput
from rocks_analysis_pipeline.stage1_state import Step
from rocks_analysis_pipeline.stage1_steps import STEP_FNS


class ProtoOutputsTest(unittest.TestCase):
    def test_every_output_step_has_a_function(self) -> None:
        for output in ProtoOutput:
            self.assertIn(output.step, STEP_FNS)

    def test_every_step_has_a_function(self) -> None:
        self.assertEqual(set(STEP_FNS), set(Step))

    def test_filenames_are_unique(self) -> None:
        self.assertEqual(len({output.filename for output in ProtoOutput}), len(ProtoOutput))

    def test_str_is_the_archive_type(self) -> None:
        self.assertEqual([str(output) for output in ProtoOutput], ["bands", "dmtracks", "events", "points"])


if __name__ == "__main__":
    unittest.main()
