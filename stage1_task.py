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

Flags are kebab-case (--runs-dir, not --runs_dir) -- deliberately
different from local_spec_sims.py/local_ssa_katydid.py's own snake_case
flags, per instruction.

Example:
    bazel run --@pypi//venv=dev //:stage1_task -- \\
        --runs-dir=/path/to/runs \\
        --run-name=myrun \\
        --subrun-id=0 \\
        --field-index=3 \\
        --yaml-config=/path/to/base.yaml \\
        --json-config=/path/to/base.json \\
        --initial-seed=0 \\
        --katydid-config=/path/to/katydid_base.yaml \\
        --noise-id=1234
    # or, in place of --noise-id:
        --noise-paths /path/to/noise_ch0.spec /path/to/noise_ch1.spec
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

    arg("--runs-dir", type=str, required=True, help="base runs directory (stage1_state.task_dir's own runs_dir)")
    arg("--run-name", type=str, required=True)
    arg("--subrun-id", type=int, required=True)
    arg("--field-index", type=int, required=True)

    arg("--yaml-config", type=str, required=True, help="base specsims yaml config, matching local_spec_sims.py's own --yaml_config")
    arg("--json-config", type=str, required=True, help="base specsims json config (fields_T/traps_A/etc.), matching local_spec_sims.py's own --json_config")
    arg(
        "--initial-seed",
        type=int,
        required=True,
        help="seed for subrun_id=0, matching local_spec_sims.py's own --initial_seed exactly "
        "(seed = initial_seed + subrun_id)",
    )
    arg("--katydid-config", type=str, required=True, help="full path to the base katydid yaml config file, matching local_ssa_katydid.py's own --katydid_config")

    # Exactly one of --noise-id/--noise-paths, restoring RunSpecSims's own
    # SQL-based noise-path resolution (run_spec_sims.py, before commit
    # ec2a84c54c3da51efe17c6b01cc06ed19542ca15 removed it) as an option
    # alongside directly-given paths, rather than only the latter -- see
    # stage1_steps.resolve_noise_paths_from_id's own doc comment. The same
    # resolved value feeds both he6-cres-spec-sims's own noise injection
    # (make_run_specsims) and Katydid's spec1 input (make_run_katydid):
    # physically the same underlying noise reference, not two independent
    # options.
    noise_group = par.add_mutually_exclusive_group(required=True)
    noise_group.add_argument(
        "--noise-id",
        type=int,
        help="run_id to look up in he6cres_runs.spec_files for noise floor (requires DB access) -- "
        "mutually exclusive with --noise-paths",
    )
    noise_group.add_argument(
        "--noise-paths",
        type=str,
        nargs=2,
        metavar=("CHANNEL_0_PATH", "CHANNEL_1_PATH"),
        help="paths to the two (per-channel) noise .spec(k) files directly, no DB access needed -- "
        "mutually exclusive with --noise-id",
    )

    arg(
        "--use-ghcss",
        action="store_true",
        help="use the ghcss Go binary instead of he6-cres-spec-sims for the specsims step (default: "
        "he6-cres-spec-sims) -- see make_run_specsims's own doc comment for why this isn't the default",
    )

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


def resolve_noise_paths(args: argparse.Namespace) -> list[str]:
    """Resolves this run's noise_paths from whichever of --noise-id/
    --noise-paths was given (argparse's own required mutually exclusive
    group guarantees exactly one is set). Split out from main() for the
    same reason as compute_skip_steps above.
    """
    if args.noise_id is not None:
        return stage1_steps.resolve_noise_paths_from_id(args.noise_id)
    return args.noise_paths


def build_step_fns(args: argparse.Namespace, noise_paths: list[str]) -> dict:
    """Builds the real, complete step_fns dict for this run: everything
    from stage1_steps.STEP_FNS, with its two placeholders (specsims_done,
    katydid_done -- see that module's own doc comment on why they're
    placeholders there) replaced by real closures built from this run's own
    CLI-provided config. Split out from main() for the same reason as
    compute_skip_steps above. noise_paths is passed in already resolved
    (see resolve_noise_paths) rather than recomputed here, so both closures
    below share the exact same resolved value rather than each
    independently re-resolving it (wasteful, and, for --noise-id, a second
    DB round-trip that could in principle resolve differently).
    """
    step_fns = dict(stage1_steps.STEP_FNS)
    step_fns["specsims_done"] = stage1_steps.make_run_specsims(
        args.yaml_config, args.json_config, args.initial_seed, noise_paths, use_ghcss=args.use_ghcss
    )
    step_fns["katydid_done"] = stage1_steps.make_run_katydid(args.katydid_config, noise_paths)
    return step_fns


def main() -> None:
    args = parse_args()
    noise_paths = resolve_noise_paths(args)
    run_stage1_task(
        Path(args.runs_dir),
        args.run_name,
        args.subrun_id,
        args.field_index,
        build_step_fns(args, noise_paths),
        skip_steps=compute_skip_steps(args),
    )


if __name__ == "__main__":
    main()
