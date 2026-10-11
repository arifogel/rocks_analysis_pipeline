"""The --stop-after flag."""

import argparse

from rocks_analysis_pipeline.stage1_state import STEPS


def add_stop_after_argument(par: argparse.ArgumentParser) -> None:
    """Adds --stop-after, whose value is a stage-1 step name, to par.

    Args:
      par: the parser to add the flag to.
    """
    par.add_argument(
        "--stop-after",
        dest="stop_after",
        choices=STEPS,
        default=None,
        help="stop each task after this stage-1 step; archives that need a later step are not produced",
    )
