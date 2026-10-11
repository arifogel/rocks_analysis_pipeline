"""Tests run_stage1_task's stop_after handling with recording step functions."""

import tempfile
import unittest
from pathlib import Path

from rocks_analysis_pipeline.checkpoints import is_checkpointed
from rocks_analysis_pipeline.stage1_state import Step, last_step, run_stage1_task, task_dir


class StopAfterTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.runs_dir = Path(tmp.name)
        self.calls: list[Step] = []
        self.step_fns = {step: (lambda d, step=step: self.calls.append(step)) for step in Step}

    def run_task(self, **kwargs) -> None:
        run_stage1_task(
            runs_dir=self.runs_dir, run_name="r", subrun_id=0, field_index=0, step_fns=self.step_fns, **kwargs
        )

    def checkpointed(self, step: Step) -> bool:
        d = task_dir(runs_dir=self.runs_dir, run_name="r", subrun_id=0, field_index=0)
        return is_checkpointed(d, step.name)

    def test_default_runs_every_step(self) -> None:
        self.run_task()
        self.assertEqual(self.calls, list(Step))

    def test_stops_after_the_named_step_inclusive(self) -> None:
        self.run_task(stop_after=Step.katydid_done)
        self.assertEqual(self.calls, [step for step in Step if step <= Step.katydid_done])
        self.assertTrue(self.checkpointed(Step.katydid_done))
        self.assertFalse(self.checkpointed(Step.katydid_log_compressed))

    def test_resume_continues_from_the_markers(self) -> None:
        self.run_task(stop_after=Step.katydid_done)
        self.calls.clear()
        self.run_task()
        self.assertEqual(self.calls, [step for step in Step if step > Step.katydid_done])

    def test_stop_step_in_skip_steps_still_stops(self) -> None:
        self.run_task(skip_steps=frozenset({Step.mc_truth_deleted}), stop_after=Step.mc_truth_deleted)
        self.assertEqual(self.calls, [step for step in Step if step < Step.mc_truth_deleted])
        self.assertFalse(self.checkpointed(Step.mc_truth_deleted))

    def test_stop_step_already_checkpointed_still_stops(self) -> None:
        self.run_task(stop_after=Step.katydid_done)
        self.calls.clear()
        self.run_task(stop_after=Step.bands_proto_done)
        self.assertEqual(self.calls, [])

    def test_first_step(self) -> None:
        self.run_task(stop_after=Step.specsims_done)
        self.assertEqual(self.calls, [Step.specsims_done])

    def test_resuming_with_markers_present_runs_nothing(self) -> None:
        self.run_task()
        self.calls.clear()
        self.run_task()
        self.assertEqual(self.calls, [])


class MissingStepFnsTest(unittest.TestCase):
    def test_raises_before_running_when_a_needed_step_has_no_function(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            step_fns = {step: (lambda d: None) for step in Step if step != Step.katydid_done}
            with self.assertRaisesRegex(ValueError, "katydid_done"):
                run_stage1_task(runs_dir=Path(tmp), run_name="r", subrun_id=0, field_index=0, step_fns=step_fns)
            self.assertFalse(any(Path(tmp).rglob(".checkpoint-*")))

    def test_steps_after_stop_after_or_in_skip_steps_need_no_function(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            step_fns = {Step.specsims_done: lambda d: None}
            run_stage1_task(
                runs_dir=Path(tmp),
                run_name="r",
                subrun_id=0,
                field_index=0,
                step_fns=step_fns,
                stop_after=Step.specsims_done,
            )
            step_fns = {step: (lambda d: None) for step in Step if step != Step.mc_truth_deleted}
            run_stage1_task(
                runs_dir=Path(tmp),
                run_name="r2",
                subrun_id=0,
                field_index=0,
                step_fns=step_fns,
                skip_steps=frozenset({Step.mc_truth_deleted}),
            )


class LastStepTest(unittest.TestCase):
    def test_none_is_the_final_step(self) -> None:
        self.assertEqual(last_step(None), Step.katydid_output_deleted)

    def test_a_step_is_itself(self) -> None:
        self.assertEqual(last_step(Step.katydid_done), Step.katydid_done)


if __name__ == "__main__":
    unittest.main()
