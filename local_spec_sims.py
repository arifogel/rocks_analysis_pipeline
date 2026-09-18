#!/usr/bin/env python3
"""
Local, parallel driver for spec-sims subruns.

For each subrun, generates all of its per-field configs via
RunSpecSimsGhcss.run(generate_configs_only=True) -- config generation only,
no he6_cres_spec_sims import at all (see run_spec_sims_ghcss.py's own module
docstring for why that's a separate file rather than an edit to
run_spec_sims.py) -- so config generation and discovery for a subrun always
happen as a single, uninterrupted unit, with correct sequential field
indices. Only *after* a subrun's configs are generated does the driver fan
the resulting per-field config paths out across a flat process pool, each
invoking ghcss's specsims binary (built from github.com/arifogel/ghcss,
cmd/specsims) as a subprocess with --config <config_path> -- the Go port of
the same Simulation(config_path).run_full() call Experiment.run_sims()'s own
per-field loop used to make -- entirely within one `bazel run` invocation.

Each job's full log is written into the same directory RunSpecSims/DAQ
already create for that (subrun, field) today:
    runs_base_dir/run_name/subrun_{id}/{i}_field_{field}T/

Example:
    bazel run --@pypi//venv=dev //:local_spec_sims -- \\
        --run_name=test1 \\
        --noise_run_id=1716 \\
        --yaml_config=/path/to/config.yaml \\
        --json_config=/path/to/config.json \\
        --num_subruns=25 \\
        --runs_base_dir=/path/to/runs \\
        --max-jobs=8

Python version note: this file is written to be compatible with Python 3.9
(e.g. `typing.Optional[int]` instead of the 3.10+ `int | None` syntax),
even though the rest of this project currently targets a newer version.
"""

import os

# This must run before numpy is imported anywhere in this process (below,
# and transitively via run_spec_sims_ghcss's own light numpy usage for field
# rounding) -- otherwise the underlying BLAS/OpenMP library has already
# latched onto its default thread count. This matters far less than it used
# to now that each worker process's actual simulation work happens in a
# separate ghcss (Go) subprocess rather than in-process via
# he6_cres_spec_sims/scipy, but it's a harmless, still-technically-correct
# precaution against the same oversubscription this process's own numpy
# import could in principle cause, so it's kept rather than removed. Using
# setdefault() rather than a plain assignment so an explicit value the user
# has already set in their environment is left alone.
for _thread_env_var in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
):
    os.environ.setdefault(_thread_env_var, "1")

import argparse  # noqa: E402
import logging  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import traceback  # noqa: E402
from concurrent.futures import Future, ProcessPoolExecutor, as_completed  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

from python.runfiles import runfiles  # noqa: E402

from logging_setup import init_logging  # noqa: E402
from run_spec_sims_ghcss import RunSpecSimsGhcss  # noqa: E402

logger = logging.getLogger(__name__)

# Canonical bzlmod repo name for the ghcss module (see MODULE.bazel's
# bazel_dep(name = "ghcss", ...)) plus the path to the specsims go_binary
# within it. If the canonical repo name or build layout for that target
# changes, update this to match -- resolve_specsims_path()'s own error
# message points back here.
SPECSIMS_RLOCATION = "ghcss+/cmd/specsims/specsims_/specsims"

# Must match exitCodeProfilingStopped in ghcss's cmd/specsims/main.go exactly.
# A specsims process exits with this code (never 0) when --profile-duration
# or a signal cuts it short before the pipeline finishes -- deliberate, not a
# failure, and _run_one_job below treats it as such rather than as a real
# error.
SPECSIMS_EXIT_CODE_PROFILING_STOPPED = 3


def resolve_specsims_path() -> str:
    r = runfiles.Create()
    specsims_path = r.Rlocation(SPECSIMS_RLOCATION)
    if specsims_path is None or not Path(specsims_path).is_file():
        raise RuntimeError(
            f"Could not resolve the ghcss specsims binary via runfiles at "
            f"'{SPECSIMS_RLOCATION}' (got: {specsims_path}). If the "
            f"canonical repo name for the ghcss module, or the go_binary's "
            f"own runfile path, has changed, update SPECSIMS_RLOCATION at "
            f"the top of this file."
        )
    return specsims_path


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
    arg(
        "--log-level",
        dest="log_level",
        type=str,
        default="INFO",
        help="root log level (e.g. DEBUG, INFO, WARNING) -- see logging_setup.init_logging",
    )
    arg(
        "--log-override",
        dest="log_override",
        type=str,
        default=None,
        help=(
            "comma-separated logger_name=LEVEL overrides for individual loggers "
            "(e.g. 'botocore=WARNING,run_spec_sims_ghcss=DEBUG') -- see logging_setup.init_logging"
        ),
    )

    return par.parse_args()


def build_jobs(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Generates every subrun's configs (sequentially, one subrun at a
    time -- see module docstring) and builds one job per resulting
    (subrun, field) config path.
    """
    seeds: list[int] = list(range(args.initial_seed, args.initial_seed + args.num_subruns))

    jobs: list[dict[str, Any]] = []
    for subrun_id in range(args.num_subruns):
        config_paths: list[Path] = RunSpecSimsGhcss(
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
            # since config_path is a real path RunSpecSimsGhcss just
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
    sent to a spawned worker process. Invokes ghcss's specsims binary as a
    subprocess with --config <config_path> -- the Go port of the same
    Simulation(config_path).run_full() call Experiment.run_sims()'s own
    per-field loop used to make -- with its stdout/stderr redirected into
    the job's natural output directory so parallel jobs don't interleave in
    the terminal.
    """
    output_dir: Path = params["output_dir"]
    log_path: Path = params["log_path"]
    config_path: Path = params["config_path"]

    output_dir.mkdir(parents=True, exist_ok=True)
    # Resolved once per worker process (not hoisted out to the parent and
    # passed in), since runfiles.Create()'s state isn't guaranteed to survive
    # being pickled to a spawned worker -- cheap enough to redo per job.
    specsims_path: str = resolve_specsims_path()

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
            print(f"Running subrun {params['subrun_id']} field {params['field_index']} ({config_path})\n\n")
            print("+++++++++++++++++++++++++++++++++++++++++++++++++")
            log_file.flush()

            command: list[str] = [specsims_path, "--config", str(config_path)]
            # Opt-in debugging hook, off by default: SPECSIMS_PROFILE_DURATION
            # (e.g. "30s") makes *every* job in this run capture a profile
            # into its own output_dir and self-terminate after that
            # duration, instead of running to completion. A run started this
            # way will not produce valid, complete spec/speck output (every
            # job stops partway through on purpose) -- it's for profiling
            # only.
            #
            # SPECSIMS_PROFILE_MODE picks which kind, and must be set
            # together with SPECSIMS_PROFILE_DURATION (silently ignored
            # otherwise) -- "trace" or "cpu", no default, and never both at
            # once. These aren't just two flavors of the same thing:
            # --trace (runtime/trace execution trace, analyzed with
            # `go tool trace -pprof=...` then `go tool pprof -top`) captures
            # scheduling latency and off-CPU blocked time -- the right tool
            # for contention between concurrently running processes, but its
            # own instrumentation overhead changes the very thing being
            # measured, and comparing a --trace-instrumented run against an
            # uninstrumented baseline produced a real, wrong conclusion once
            # already in this project's own profiling history. --cpuprofile
            # (runtime/pprof, analyzed directly with `go tool pprof -top` --
            # no trace-extraction step needed, since it's already pprof
            # format) samples actual on-CPU execution time instead, a more
            # direct proxy for wall-clock cost, and is the one to reach for
            # when the question is "which function is actually expensive"
            # rather than "are processes contending with each other." Kept
            # as separate, mutually exclusive modes (never both flags on the
            # same command) specifically so this hook can't be used to
            # repeat that same mistake.
            profile_duration = os.environ.get("SPECSIMS_PROFILE_DURATION")
            profile_mode = os.environ.get("SPECSIMS_PROFILE_MODE")
            if profile_duration and profile_mode == "trace":
                command += [
                    "--trace",
                    str(output_dir / "trace.out"),
                    "--profile-duration",
                    profile_duration,
                ]
            elif profile_duration and profile_mode == "cpu":
                command += [
                    "--cpuprofile",
                    str(output_dir / "cpu.prof"),
                    "--profile-duration",
                    profile_duration,
                ]
            elif profile_duration and not profile_mode:
                raise ValueError(
                    "SPECSIMS_PROFILE_DURATION is set but SPECSIMS_PROFILE_MODE isn't -- "
                    "set it to 'trace' or 'cpu' to pick which profile to capture."
                )

            # check=True: a nonzero exit raises CalledProcessError, caught
            # below. The subprocess's own stdout/stderr (redirected here,
            # not captured/buffered by Python) already went straight into
            # log_file, so there's nothing further to print from a
            # successful or failed run beyond the exception itself.
            try:
                subprocess.run(
                    command,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                if e.returncode == SPECSIMS_EXIT_CODE_PROFILING_STOPPED:
                    # Deliberate (--profile-duration or a signal cut this
                    # job short on purpose), not a failure -- see
                    # SPECSIMS_EXIT_CODE_PROFILING_STOPPED's own comment.
                    # Returns normally rather than re-raising so this
                    # doesn't get counted as a failed job by main()'s own
                    # summary below.
                    print(
                        f"\nsubrun {params['subrun_id']} field {params['field_index']} "
                        f"stopped on purpose for profiling (exit code {e.returncode})\n"
                    )
                    return
                raise

            print(f"\nsubrun {params['subrun_id']} field {params['field_index']} DONE\n")
        except Exception:
            traceback.print_exc()
            raise


def main() -> None:
    args: argparse.Namespace = parse_args()
    init_logging(args.log_level, args.log_override)

    run_dir: Path = Path(args.runs_base_dir) / args.run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    jobs: list[dict[str, Any]] = build_jobs(args)

    if args.dry_run:
        for job in jobs:
            logger.info(
                "[dry_run] subrun %s field %s: config=%s output_dir=%s",
                job["subrun_id"],
                job["field_index"],
                job["config_path"],
                job["output_dir"],
            )
        return

    for job in jobs:
        job["output_dir"].mkdir(parents=True, exist_ok=True)

    max_jobs: int | None = args.max_jobs
    logger.info("Running %d job(s) with max_jobs=%s", len(jobs), max_jobs or "(cpu count)")

    failures: list[str] = []
    with ProcessPoolExecutor(max_workers=max_jobs) as pool:
        futures: dict[Future, dict[str, Any]] = {pool.submit(_run_one_job, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            job_label: str = f"subrun {job['subrun_id']} field {job['field_index']}"
            try:
                future.result()
                logger.info("%s: OK (log: %s)", job_label, job["log_path"])
            except Exception as e:
                failures.append(job_label)
                logger.error("%s: FAILED (%s) (log: %s)", job_label, e, job["log_path"])

    if failures:
        logger.error("%d of %d job(s) failed: %s", len(failures), len(jobs), failures)
        sys.exit(1)

    logger.info("All %d job(s) completed successfully.", len(jobs))


if __name__ == "__main__":
    main()
