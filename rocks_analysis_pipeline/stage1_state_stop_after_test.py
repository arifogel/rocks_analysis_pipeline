"""Tests run_stage1_task's stop_after handling with recording step functions."""

import tempfile
import unittest
from pathlib import Path

from rocks_analysis_pipeline.checkpoints import is_checkpointed
from rocks_analysis_pipeline.stage1_state import STEPS, run_stage1_task, task_dir


class StopAfterTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.runs_dir = Path(tmp.name)
        self.calls: list[str] = []
        self.step_fns = {step: (lambda d, step=step: self.calls.append(step)) for step in STEPS}

    def run_task(self, **kwargs) -> None:
        run_stage1_task(
            runs_dir=self.runs_dir, run_name="r", subrun_id=0, field_index=0, step_fns=self.step_fns, **kwargs
        )

    def checkpointed(self, step: str) -> bool:
        d = task_dir(runs_dir=self.runs_dir, run_name="r", subrun_id=0, field_index=0)
        return is_checkpointed(d, step)

    def test_default_runs_every_step(self) -> None:
        self.run_task()
        self.assertEqual(self.calls, STEPS)

    def test_stops_after_the_named_step_inclusive(self) -> None:
        self.run_task(stop_after="katydid_done")
        self.assertEqual(self.calls, STEPS[: STEPS.index("katydid_done") + 1])
        self.assertTrue(self.checkpointed("katydid_done"))
        self.assertFalse(self.checkpointed("katydid_log_compressed"))

    def test_resume_continues_from_the_markers(self) -> None:
        self.run_task(stop_after="katydid_done")
        self.calls.clear()
        self.run_task()
        self.assertEqual(self.calls, STEPS[STEPS.index("katydid_done") + 1 :])

    def test_stop_step_in_skip_steps_still_stops(self) -> None:
        self.run_task(skip_steps=frozenset({"mc_truth_deleted"}), stop_after="mc_truth_deleted")
        self.assertEqual(self.calls, STEPS[: STEPS.index("mc_truth_deleted")])
        self.assertFalse(self.checkpointed("mc_truth_deleted"))

    def test_stop_step_already_checkpointed_still_stops(self) -> None:
        self.run_task(stop_after="katydid_done")
        self.calls.clear()
        self.run_task(stop_after="bands_proto_done")
        self.assertEqual(self.calls, [])

    def test_first_step(self) -> None:
        self.run_task(stop_after=STEPS[0])
        self.assertEqual(self.calls, [STEPS[0]])

    def test_unknown_step_raises_before_running(self) -> None:
        with self.assertRaises(ValueError):
            self.run_task(stop_after="nope")
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
