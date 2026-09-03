#!/usr/bin/env python3
"""
Local, parallel driver for spec-sims subruns.

For each subrun, generates all of its per-field configs via
RunSpecSims.run(generate_configs_only=True) -- exactly the same call path
the single-process code already uses, just stopped before it also runs
each field sequentially -- so config generation and discovery for a subrun
always happen as a single, uninterrupted unit, with correct sequential
field indices. Only *after* a subrun's configs are generated does the
driver fan the resulting per-field config paths out across a flat process
pool, each calling he6_cres_spec_sims.simulation.Simulation(config_path)
.run_full() directly -- the same call Experiment.run_sims()'s own
per-field loop makes -- entirely within one `bazel run` invocation.

generate_configs_only is an explicit, default-False parameter added to
RunSpecSims.run()/Experiment.__init__ for exactly this purpose (see
run_spec_sims.py / he6-cres-spec-sims's experiment.py). It does not change
behavior for any existing caller.

Each job's full log is written into the same directory RunSpecSims/DAQ
already create for that (subrun, field) today:
    runs_base_dir/run_name/subrun_{id}/{i}_field_{field}T/

Requires he6-cres-spec-sims's directory-creation calls to be idempotent
(mkdir(exist_ok=True)) rather than racily check-then-creating, since many
fields of the same subrun can now run concurrently and share a parent
directory. See experiment.py/DAQ.py/simulation.py.

Example:
    bazel run --@pypi//venv=dev //:local_spec_sims -- \\
        --run_name=test1 \\
        --noise_run_id=1716 \\
        --yaml_config=/Users/arifogel/creswork/sims/config_files/example_full_pileup.yaml \\
        --json_config=/Users/arifogel/creswork/sims/config_files/example_full_pileup.json \\
        --num_subruns=25 \\
        --runs_base_dir=/Users/arifogel/creswork/sims/runs \\
        --max-jobs=8

Python version note: this file is written to be compatible with Python 3.9
(e.g. `typing.Optional[int]` instead of the 3.10+ `int | None` syntax),
even though the rest of this project currently targets a newer version.
"""
import os

# This must run before numpy/scipy are imported anywhere in this process
# (below, and transitively via he6_cres_spec_sims once RunSpecSims is
# imported) -- otherwise the underlying BLAS/OpenMP library has already
# latched onto its default thread count. Without this, each worker process
# spawned by ProcessPoolExecutor below would *also* try to use every
# logical core for its own numpy/scipy calls, so N worker processes x N
# BLAS threads each massively oversubscribes an N-core machine: every core
# shows 100% busy, but almost all of it is contention/context-switching
# rather than real work. Using setdefault() rather than a plain assignment
# so an explicit value the user has already set in their environment is
# left alone.
for _thread_env_var in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
):
    os.environ.setdefault(_thread_env_var, "1")

import argparse
import sys
import traceback
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List, Optional

import he6_cres_spec_sims.simulation as he6_simulation
from run_spec_sims import RunSpecSims


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-r", "--run_name", type=str, required=True, help="run name")
    arg(
        "-nid",
        "--noise_run_id",
        type=int,
        required=True,
        help=(
            "run_id to use for noise floor. Kept for CLI parity with "
            "sbatch_spec_sims.py / run_spec_sims.py; not currently used by "
            "the spec-sims code path (noise paths come from the yaml config)."
        ),
    )
    arg(
        "-y",
        "--yaml_config",
        type=str,
        required=True,
        help="base .yaml spec-sims config file to be run",
    )
    arg(
        "-j",
        "--json_config",
        type=str,
        required=True,
        help="base .json spec-sims config file to be run",
    )
    arg(
        "-n",
        "--num_subruns",
        type=int,
        default=1,
        help="number of subruns. Each subrun is identical except for the seed",
    )
    arg(
        "-s0",
        "--initial_seed",
        type=int,
        default=0,
        help="seed for subrun_id = 0",
    )
    arg(
        "-rb",
        "--runs_base_dir",
        type=str,
        required=True,
        help="base output directory for runs",
    )
    arg(
        "--max-jobs",
        dest="max_jobs",
        type=int,
        default=None,
        help="max number of (subrun, field) jobs to run concurrently (default: os.cpu_count())",
    )
    arg(
        "-d",
        "--dry_run",
        action="store_true",
        help="print the planned jobs without running them",
    )

    return par.parse_args()


def build_jobs(args: argparse.Namespace) -> List[dict[str, Any]]:
    """Generates every subrun's configs (sequentially, one subrun at a
    time -- see module docstring) and builds one job per resulting
    (subrun, field) config path.
    """
    seeds: list[int] = list(
        range(args.initial_seed, args.initial_seed + args.num_subruns)
    )

    jobs: list[dict[str, Any]] = []
    for subrun_id in range(args.num_subruns):
        config_paths: List[Path] = RunSpecSims(
            run_name=args.run_name,
            subrun_id=subrun_id,
            noise_run_id=args.noise_run_id,
            yaml_config=args.yaml_config,
            json_config=args.json_config,
            seed=seeds[subrun_id],
            runs_base_dir=args.runs_base_dir,
        ).run(generate_configs_only=True)

        for field_index, config_path in enumerate(config_paths):
            # Matches Results.get_path_name()'s own computation exactly,
            # since config_path is a real path RunSpecSims/Experiment just
            # generated (not a predicted/guessed one).
            output_dir: Path = config_path.parent / config_path.stem
            log_path: Path = output_dir / "local_spec_sims.log"

            jobs.append(
                {
                    "subrun_id": subrun_id,
                    "field_index": field_index,
                    "config_path": config_path,
                    "output_dir": output_dir,
                    "log_path": log_path,
                }
            )
    return jobs


def _run_one_job(params: dict[str, Any]) -> None:
    """Runs a single (subrun, field) job in this worker process.

    This is a top-level function (not a closure) so it can be pickled and
    sent to a spawned worker process. Calls Simulation(config_path).run_full()
    directly -- the same call Experiment.run_sims()'s own per-field loop
    makes -- with stdout/stderr redirected into the job's natural output
    directory so parallel jobs don't interleave in the terminal.
    """
    output_dir: Path = params["output_dir"]
    log_path: Path = params["log_path"]
    config_path: Path = params["config_path"]

    output_dir.mkdir(parents=True, exist_ok=True)
    # buffering=1 (line-buffered): without this, writes to a redirected
    # sys.stdout are fully block-buffered rather than line-buffered, so a
    # log file tailed while the job is still running can appear to lag far
    # behind (or show nothing at all) even though the job is progressing
    # normally -- everything gets flushed eventually, but only once the
    # internal buffer fills or the file is closed.
    with open(log_path, "w", buffering=1) as log_file:
        sys.stdout = log_file
        sys.stderr = log_file
        try:
            print("+++++++++++++++++++++++++++++++++++++++++++++++++\n\n")
            print(
                f"Running subrun {params['subrun_id']} field "
                f"{params['field_index']} ({config_path})\n\n"
            )
            print("+++++++++++++++++++++++++++++++++++++++++++++++++")

            simulation = he6_simulation.Simulation(config_path)
            simulation.run_full()

            print(
                f"\nsubrun {params['subrun_id']} field {params['field_index']} DONE\n"
            )
        except Exception:
            traceback.print_exc()
            raise


def main() -> None:
    args: argparse.Namespace = parse_args()

    run_dir: Path = Path(args.runs_base_dir) / args.run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    jobs: List[dict[str, Any]] = build_jobs(args)

    if args.dry_run:
        for job in jobs:
            print(
                f"[dry_run] subrun {job['subrun_id']} field {job['field_index']}: "
                f"config={job['config_path']} output_dir={job['output_dir']}"
            )
        return

    for job in jobs:
        job["output_dir"].mkdir(parents=True, exist_ok=True)

    max_jobs: Optional[int] = args.max_jobs
    print(f"Running {len(jobs)} job(s) with max_jobs={max_jobs or '(cpu count)'}")

    failures: list[str] = []
    with ProcessPoolExecutor(max_workers=max_jobs) as pool:
        futures: dict[Future, dict[str, Any]] = {
            pool.submit(_run_one_job, job): job for job in jobs
        }
        for future in as_completed(futures):
            job = futures[future]
            job_label: str = f"subrun {job['subrun_id']} field {job['field_index']}"
            try:
                future.result()
                print(f"{job_label}: OK (log: {job['log_path']})")
            except Exception as e:
                failures.append(job_label)
                print(f"{job_label}: FAILED ({e}) (log: {job['log_path']})")

    if failures:
        print(f"\n{len(failures)} of {len(jobs)} job(s) failed: {failures}")
        sys.exit(1)

    print(f"\nAll {len(jobs)} job(s) completed successfully.")


if __name__ == "__main__":
    main()