"""The --stop-after flag."""

import argparse

from rocks_analysis_pipeline.stage1_state import Step


def _parse_step(name: str) -> Step:
    try:
        return Step[name]
    except KeyError:
        raise argparse.ArgumentTypeError(f"unknown step {name!r}; expected one of {', '.join(map(str, Step))}")


def add_stop_after_argument(par: argparse.ArgumentParser) -> None:
    """Adds --stop-after, whose value is a stage-1 step, to par.

    Args:
      par: the parser to add the flag to.
    """
    par.add_argument(
        "--stop-after",
        dest="stop_after",
        type=_parse_step,
        metavar="STEP",
        default=None,
        help="stop each task after this stage-1 step; archives that need a later step are not produced. "
        f"One of: {', '.join(map(str, Step))}",
    )
