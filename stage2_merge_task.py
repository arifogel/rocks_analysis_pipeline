#!/usr/bin/env python3
"""
Thin CLI driver for stage 2's own merge step (see stage2_merge.py's own
module doc comment for the real logic and reasoning) -- exists as its own,
separate entry point for the wulf case: stage 1 orchestrated separately
from local_ssa (see local_ssa.py's own module doc comment on why), needing
stage 2 run as its own, standalone step against whatever stage-1 output
already exists on disk, rather than only ever in-process from local_ssa's
own main().

Flags match local_ssa.py's own --runs-dir/--run-name exactly, for the same
reason local_ssa.py's own flags match stage1_task.py's -- a value copied
from one CLI's own --help works unchanged on the other.

Example:
    bazel run --@pypi//venv=dev //:stage2_merge_task -- \\
        --runs-dir=/path/to/runs \\
        --run-name=test1
"""

import argparse
import logging
from pathlib import Path

from logging_setup import init_logging
from stage2_merge import run_stage2_merge

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("--runs-dir", type=str, required=True, help="base runs directory, matching local_ssa.py's own --runs-dir")
    arg("--run-name", type=str, required=True, help="run name, matching local_ssa.py's own --run-name")
    arg("--log-level", type=str, default="INFO", help="root log level -- see logging_setup.init_logging")
    arg(
        "--log-override",
        type=str,
        default=None,
        help="comma-separated logger_name=LEVEL overrides -- see logging_setup.init_logging",
    )

    return par.parse_args()


def main() -> None:
    args = parse_args()
    init_logging(args.log_level, args.log_override)
    run_stage2_merge(Path(args.runs_dir), args.run_name)


if __name__ == "__main__":
    main()
