"""The --keep-<x> flags, each keeping the output one deletion step would remove."""

import argparse
import enum
from collections.abc import Set

from rocks_analysis_pipeline.stage1_state import Step


class KeepFlag(enum.Enum):
    """A --keep-<x> flag. Passing it skips `step` and leaves what that step deletes in place."""

    uncompressed_specsims_log = (
        Step.uncompressed_specsims_log_deleted,
        "don't delete the uncompressed specsims.log after compressing it",
    )
    mc_truth = (Step.mc_truth_deleted, "don't delete bands.csv/dmtracks.csv after converting them to proto")
    uncompressed_katydid_log = (
        Step.uncompressed_katydid_log_deleted,
        "don't delete the uncompressed katydid.log after compressing it",
    )
    specsims_output = (Step.specsims_output_deleted, "don't delete the .speck files after Katydid has consumed them")
    katydid_output = (
        Step.katydid_output_deleted,
        "don't delete the .root/slew-times files after converting them to proto",
    )

    def __init__(self, step: Step, help: str) -> None:
        self.step = step
        self.help = help

    @property
    def option(self) -> str:
        return "--keep-" + self.name.replace("_", "-")

    @property
    def dest(self) -> str:
        return "keep_" + self.name


def add_keep_arguments(par: argparse.ArgumentParser) -> None:
    """Adds a --keep-<x> flag per KeepFlag, plus --keep-all, to par.

    Args:
      par: the parser to add the flags to.
    """
    for flag in KeepFlag:
        par.add_argument(flag.option, dest=flag.dest, action="store_true", help=flag.help)
    par.add_argument(
        "--keep-all",
        dest="keep_all",
        action="store_true",
        help="equivalent to passing every individual --keep-<x> flag",
    )


def kept_flags(args: argparse.Namespace) -> Set[KeepFlag]:
    """Returns the flags set on args, which is every flag when --keep-all is set.

    Args:
      args: parsed arguments from a parser given add_keep_arguments.

    Returns:
      The kept flags.
    """
    if args.keep_all:
        return frozenset(KeepFlag)
    return frozenset(flag for flag in KeepFlag if getattr(args, flag.dest))


def keep_options(args: argparse.Namespace) -> list[str]:
    """Returns the command-line options that reproduce args' kept flags.

    Args:
      args: parsed arguments from a parser given add_keep_arguments.

    Returns:
      One option per kept flag, in KeepFlag order.
    """
    kept = kept_flags(args)
    return [flag.option for flag in KeepFlag if flag in kept]
