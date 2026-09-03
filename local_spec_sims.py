#!/usr/bin/env python3
"""
Local, parallel driver for spec-sims subruns.

Explodes a (yaml_config, json_config) pair describing N fields into
num_subruns * N independent (subrun, field) jobs -- one field each -- and
runs all of them across a single flat process pool, entirely within one
`bazel run` invocation. Each job calls RunSpecSims.run() completely
unmodified (the same class the single-field bazel target already uses), so
a given (subrun, field)'s output is identical to running it individually.

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
import json
import subprocess as sp
import sys
import traceback
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

import numpy as np

from rocks_utility import get_pst_time, log_file_break, set_permissions
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
        help=(
            "base .json spec-sims config file to be run. Its fields_T/"
            "traps_A are exploded into one single-field job each."
        ),
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


def load_base_json_config(json_config_path: str) -> dict[str, Any]:
    """Loads the base json_config file once, returning the parsed dict."""
    with open(json_config_path, "r") as f:
        return json.load(f)


def field_dir_name(field_value: float) -> str:
    """Reproduces the exact directory-name convention that
    Experiment.create_configs_for_experiment() / Results.save() use for a
    single-field config, so the driver can predict a (subrun, field) job's
    output directory before running it.

    A single-field job's fields_T list always has length 1, so it always
    gets index 0 within Experiment's own enumeration -- hence the fixed
    "0_field_..." prefix here, regardless of the field's original position
    in the un-exploded fields_T list.
    """
    rounded_field: np.float64 = np.around(field_value, 6)
    return "0_field_{}T".format(rounded_field)


def build_jobs(
    args: argparse.Namespace, run_dir: Path, base_json: dict[str, Any]
) -> list[dict[str, Any]]:
    """Builds one job per (subrun_id, field_index), by slicing the base
    json_config's fields_T/traps_A down to a single field each.

    rand_seeds is not sliced here since RunSpecSims.run() overwrites it
    unconditionally, based on the per-subrun seed and the (sliced) length of
    fields_T -- so it doesn't matter what's in the base json_config.
    """
    fields_t: list[float] = base_json["fields_T"]
    traps_a: list[float] = base_json["traps_A"]
    if len(fields_t) != len(traps_a):
        raise ValueError(
            f"fields_T (len={len(fields_t)}) and traps_A (len={len(traps_a)}) "
            "must be the same length in the json_config."
        )

    seeds: list[int] = list(
        range(args.initial_seed, args.initial_seed + args.num_subruns)
    )

    jobs: list[dict[str, Any]] = []
    for subrun_id in range(args.num_subruns):
        subrun_dir: Path = run_dir / f"subrun_{subrun_id}"
        job_config_dir: Path = subrun_dir / "_driver_job_configs"

        for field_index, (field_value, trap_value) in enumerate(
            zip(fields_t, traps_a)
        ):
            per_field_json: dict[str, Any] = dict(base_json)
            per_field_json["fields_T"] = [field_value]
            per_field_json["traps_A"] = [trap_value]

            job_config_path: Path = job_config_dir / f"field_{field_index:02d}.json"
            output_dir: Path = subrun_dir / field_dir_name(field_value)
            log_path: Path = output_dir / "local_spec_sims.log"

            jobs.append(
                {
                    "run_name": args.run_name,
                    "subrun_id": subrun_id,
                    "field_index": field_index,
                    "noise_run_id": args.noise_run_id,
                    "yaml_config": args.yaml_config,
                    "json_config_path": job_config_path,
                    "json_config_contents": per_field_json,
                    "seed": seeds[subrun_id],
                    "runs_base_dir": args.runs_base_dir,
                    "output_dir": output_dir,
                    "log_path": log_path,
                }
            )
    return jobs


def _run_one_job(params: dict[str, Any]) -> None:
    """Runs a single (subrun, field) job in this worker process.

    This is a top-level function (not a closure) so it can be pickled and
    sent to a spawned worker process. Writes this job's single-field
    json_config to disk, then calls RunSpecSims.run() completely
    unmodified -- the same call a single-field bazel run would make -- with
    stdout/stderr redirected into the job's natural output directory (see
    field_dir_name()) so parallel jobs don't interleave in the terminal.
    """
    output_dir: Path = params["output_dir"]
    log_path: Path = params["log_path"]
    job_config_path: Path = params["json_config_path"]

    job_config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(job_config_path, "w") as config_file:
        json.dump(params["json_config_contents"], config_file)

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as log_file:
        sys.stdout = log_file
        sys.stderr = log_file
        try:
            sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

            print(f"\nRunning spec-sims. STARTING at PST time: {get_pst_time()}\n")
            print(
                f"\nProcessing: subrun_id: {params['subrun_id']}, "
                f"field_index: {params['field_index']}.\n"
            )
            sys.stdout.flush()

            set_permissions()

            run_spec_sims = RunSpecSims(
                run_name=params["run_name"],
                subrun_id=params["subrun_id"],
                noise_run_id=params["noise_run_id"],
                yaml_config=params["yaml_config"],
                json_config=str(job_config_path),
                seed=params["seed"],
                runs_base_dir=params["runs_base_dir"],
            )

            print(
                f"\nRunning spec-sims on {params['run_name']} "
                f"{params['subrun_id']} field {params['field_index']} "
                f"DONE at PST time: {get_pst_time()}\n"
            )
            run_spec_sims.run()

            log_file_break()
        except Exception:
            traceback.print_exc()
            raise


def main() -> None:
    args: argparse.Namespace = parse_args()

    run_dir: Path = Path(args.runs_base_dir) / args.run_name
    base_json: dict[str, Any] = load_base_json_config(args.json_config)

    jobs: list[dict[str, Any]] = build_jobs(args, run_dir, base_json)

    if args.dry_run:
        print(f"[dry_run] Would create: {run_dir}")
        for job in jobs:
            print(
                f"[dry_run] subrun {job['subrun_id']} field {job['field_index']}: "
                f"seed={job['seed']} output_dir={job['output_dir']}"
            )
        return

    # Pre-create shared/output directories once, up front, before spawning
    # any workers. he6-cres-spec-sims's own directory creation is now
    # idempotent (mkdir(parents=True, exist_ok=True)) rather than racily
    # check-then-creating or destructively deleting an existing directory,
    # but there's no reason to lean on that alone when the driver can just
    # as easily ensure these exist before any worker starts.
    run_dir.mkdir(parents=True, exist_ok=True)
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