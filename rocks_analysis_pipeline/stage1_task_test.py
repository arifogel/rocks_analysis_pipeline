"""Tests that stage1_task builds a function for every step."""

import argparse
import unittest

from rocks_analysis_pipeline.stage1_state import Step
from rocks_analysis_pipeline.stage1_task import build_step_fns


class BuildStepFnsTest(unittest.TestCase):
    def test_covers_every_step(self) -> None:
        args = argparse.Namespace(
            yaml_config="y.yaml", json_config="j.json", initial_seed=0, use_ghcss=False, katydid_config="k.yaml"
        )
        self.assertEqual(set(build_step_fns(args, ["n0", "n1"])), set(Step))


if __name__ == "__main__":
    unittest.main()
