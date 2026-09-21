#!/usr/bin/env python3
"""
Single-task entry point for stage 1: runs the full specsims -> Katydid ->
proto+zstd pipeline (see stage1_state.py/stage1_steps.py) for one
(run_name, subrun_id, field_index) task, resuming correctly from wherever a
previous attempt left off.

Analogous to run_spec_sims_ghcss.py/local_ssa_katydid.py's own single-
config/single-row logic, but for the combined stage-1 pipeline -- this is
the thing local_stage1.py (not yet written) will fan out across a process
pool / wulf nodes, one invocation per task.

Example:
    bazel run --@pypi//venv=dev //:stage1_task -- \\
        --runs_dir=/path/to/runs \\
        --run_name=myrun \\
        --subrun_id=0 \\
        --field_index=3
"""

import argparse
from pathlib import Path

import stage1_steps
from stage1_state import (
    STEP_KATYDID_OUTPUT_DELETED,
    STEP_MC_TRUTH_DELETED,
    STEP_SPECSIMS_OUTPUT_DELETED,
    STEP_UNCOMPRESSED_LOG_DELETED,
    run_stage1_task,
)

# Maps each --keep-<x> flag's argparse dest to the one delete step it nops
# out (both the delete action itself and that step's own checkpoint --
# see run_stage1_task's own skip_steps parameter). One entry per delete
# step in STEPS; production steps are never skippable this way.
KEEP_FLAG_STEPS: dict[str, str] = {
    "keep_uncompressed_log": STEP_UNCOMPRESSED_LOG_DELETED,
    "keep_mc_truth": STEP_MC_TRUTH_DELETED,
    "keep_specsims_output": STEP_SPECSIMS_OUTPUT_DELETED,
    "keep_katydid_output": STEP_KATYDID_OUTPUT_DELETED,
}


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-rd", "--runs_dir", type=str, required=True, help="base runs directory (stage1_state.task_dir's own runs_dir)")
    arg("-r", "--run_name", type=str, required=True)
    arg("-s", "--subrun_id", type=int, required=True)
    arg("-f", "--field_index", type=int, required=True)

    arg(
        "--keep-uncompressed-log",
        dest="keep_uncompressed_log",
        action="store_true",
        help="don't delete the uncompressed log after compressing it",
    )
    arg(
        "--keep-mc-truth",
        dest="keep_mc_truth",
        action="store_true",
        help="don't delete bands.csv/dmtracks.csv after converting them to proto",
    )
    arg(
        "--keep-specsims-output",
        dest="keep_specsims_output",
        action="store_true",
        help="don't delete the .speck files after Katydid has consumed them",
    )
    arg(
        "--keep-katydid-output",
        dest="keep_katydid_output",
        action="store_true",
        help="don't delete the .root/slew-times files after converting them to proto",
    )
    arg(
        "--keep-all",
        dest="keep_all",
        action="store_true",
        help="equivalent to passing every individual --keep-<x> flag above",
    )

    return par.parse_args()


def compute_skip_steps(args: argparse.Namespace) -> frozenset[str]:
    """Translates the parsed --keep-<x>/--keep-all flags into the set of
    step names to pass as run_stage1_task's own skip_steps. Split out from
    main() so this translation is directly testable without going through
    argparse/sys.argv.
    """
    if args.keep_all:
        return frozenset(KEEP_FLAG_STEPS.values())
    return frozenset(step for flag_dest, step in KEEP_FLAG_STEPS.items() if getattr(args, flag_dest))


def main() -> None:
    args = parse_args()
    run_stage1_task(
        Path(args.runs_dir),
        args.run_name,
        args.subrun_id,
        args.field_index,
        stage1_steps.STEP_FNS,
        skip_steps=compute_skip_steps(args),
    )


if __name__ == "__main__":
    main()
