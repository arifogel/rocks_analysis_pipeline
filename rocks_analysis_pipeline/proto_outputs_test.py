"""Tests the per-output step mapping and partition_types."""

import unittest

from rocks_analysis_pipeline.proto_outputs import ARCHIVE_TYPES, PROTO_OUTPUTS, partition_types
from rocks_analysis_pipeline.stage1_state import STEPS
from rocks_analysis_pipeline.stage1_steps import STEP_FNS


class ProtoOutputsTest(unittest.TestCase):
    def test_every_output_step_is_a_stage1_step_with_a_function(self) -> None:
        for output in PROTO_OUTPUTS:
            self.assertIn(output.step, STEPS)
            self.assertIn(output.step, STEP_FNS)

    def test_none_returns_every_type(self) -> None:
        self.assertEqual(partition_types(None), (ARCHIVE_TYPES, ()))

    def test_before_any_output_step_withholds_everything(self) -> None:
        self.assertEqual(partition_types("specsims_done"), ((), ARCHIVE_TYPES))

    def test_stopping_at_an_output_step_includes_it(self) -> None:
        self.assertEqual(partition_types("bands_proto_done"), (("bands",), ("dmtracks", "events", "points")))

    def test_katydid_done_yields_the_specsims_types(self) -> None:
        self.assertEqual(partition_types("katydid_done"), (("bands", "dmtracks"), ("events", "points")))

    def test_events_and_points_boundaries(self) -> None:
        self.assertEqual(partition_types("events_proto_done"), (("bands", "dmtracks", "events"), ("points",)))
        self.assertEqual(partition_types("points_proto_done"), (ARCHIVE_TYPES, ()))

    def test_last_step_yields_every_type(self) -> None:
        self.assertEqual(partition_types(STEPS[-1]), (ARCHIVE_TYPES, ()))

    def test_unknown_step_raises(self) -> None:
        with self.assertRaises(ValueError):
            partition_types("nope")


if __name__ == "__main__":
    unittest.main()
