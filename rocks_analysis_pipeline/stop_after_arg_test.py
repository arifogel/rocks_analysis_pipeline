"""Tests the --stop-after flag."""

import argparse
import contextlib
import io
import unittest

from rocks_analysis_pipeline.stage1_state import STEPS
from rocks_analysis_pipeline.stop_after_arg import add_stop_after_argument


class StopAfterArgTest(unittest.TestCase):
    def setUp(self) -> None:
        self.par = argparse.ArgumentParser()
        add_stop_after_argument(self.par)

    def test_defaults_to_none(self) -> None:
        self.assertIsNone(self.par.parse_args([]).stop_after)

    def test_accepts_every_step(self) -> None:
        for step in STEPS:
            self.assertEqual(self.par.parse_args([f"--stop-after={step}"]).stop_after, step)

    def test_rejects_unknown_step(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.par.parse_args(["--stop-after=nope"])


if __name__ == "__main__":
    unittest.main()
