"""Tests the --keep-<x> flags."""

import argparse
import unittest

from rocks_analysis_pipeline.keep_flags import KeepFlag, add_keep_arguments, keep_options, kept_flags
from rocks_analysis_pipeline.stage1_state import Step


class KeepFlagsTest(unittest.TestCase):
    def parse(self, *argv: str) -> argparse.Namespace:
        par = argparse.ArgumentParser()
        add_keep_arguments(par)
        return par.parse_args(argv)

    def test_default_keeps_nothing(self) -> None:
        args = self.parse()
        self.assertEqual(kept_flags(args), frozenset())
        self.assertEqual(keep_options(args), [])

    def test_individual_flag(self) -> None:
        args = self.parse("--keep-mc-truth")
        self.assertEqual(kept_flags(args), {KeepFlag.mc_truth})
        self.assertEqual(keep_options(args), ["--keep-mc-truth"])

    def test_keep_all_keeps_every_flag(self) -> None:
        args = self.parse("--keep-all")
        self.assertEqual(kept_flags(args), frozenset(KeepFlag))
        self.assertEqual(keep_options(args), [flag.option for flag in KeepFlag])

    def test_options_round_trip(self) -> None:
        args = self.parse("--keep-specsims-output", "--keep-uncompressed-specsims-log")
        self.assertEqual(kept_flags(self.parse(*keep_options(args))), kept_flags(args))

    def test_option_spellings(self) -> None:
        self.assertEqual(
            [flag.option for flag in KeepFlag],
            [
                "--keep-uncompressed-specsims-log",
                "--keep-mc-truth",
                "--keep-uncompressed-katydid-log",
                "--keep-specsims-output",
                "--keep-katydid-output",
            ],
        )

    def test_steps_are_distinct_deletion_steps(self) -> None:
        steps = [flag.step for flag in KeepFlag]
        self.assertEqual(len(set(steps)), len(steps))
        for step in steps:
            self.assertTrue(step.name.endswith("_deleted"), step)
            self.assertIsInstance(step, Step)


if __name__ == "__main__":
    unittest.main()
