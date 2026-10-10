#!/usr/bin/env python3
"""
Local, parallel orchestration driver for this project's ssa (spec-sims and
analysis) pipeline: fans out stage1_task across every (subrun_id,
field_index) task for a run via a ThreadPoolExecutor, then, once every
task has completed successfully, merges that run's bands/dmtracks/
events/points into runs_dir/run_name/{bands,dmtracks,events,points}.pb.zst
by calling stage2_merge.run_stage2_merge directly in-process.

Crash isolation comes from stage1_task running as a fresh subprocess, once
per task, not from this orchestrator's worker being a separate process: a
hard crash (segfault, OOM-kill) inside one task's Simulation.run_full() or
Katydid invocation only ever kills that task's subprocess. A
ThreadPoolExecutor is the right tool given that: each worker thread mostly
just waits on a subprocess, so it's I/O-bound rather than CPU-bound in
this process.

Each task's subprocess is this process's sys.executable running `-m
rocks_analysis_pipeline.stage1_task`, which needs no resolved file path --
Python's import machinery finds the module directly. This process's venv
already carries every package stage1_task needs (numpy, uproot,
he6-cres-spec-sims, etc.), so no separate venv or warm-up step is needed
anywhere: there is only ever the one venv, set up once by bazel's launcher
before any tasks run.

Because multiple worker threads share this one process's root logger, each
log record is tagged with which task its thread is currently on, via a
contextvars.ContextVar and a logging.Filter, so concurrent tasks'
"starting"/"finished" messages stay distinguishable on this orchestrator's
shared console. This is this orchestrator's brief per-task status only;
stage1_task's full per-task detail (the katydid command, row counts, etc.)
goes to stage1_task.log, written by stage1_task's main() every time it
runs as a fresh subprocess.

Flags are kebab-case, matching stage1_task's convention. For every flag
this shares a concept with (--runs-dir, --run-name, --yaml-config,
--json-config, --initial-seed, --katydid-config, --noise-id/--noise-paths,
--use-ghcss, --log-level, --log-override, every --keep-<x>), the name here
is identical to stage1_task's, not just similarly named, so a value
copied from one CLI's --help works unchanged on the other.

Example:
    bazel run --@pypi//venv=dev //:local_ssa -- \\
        --run-name=test1 \\
        --runs-dir=/path/to/runs \\
        --yaml-config=/path/to/base.yaml \\
        --json-config=/path/to/base.json \\
        --num-subruns=25 \\
        --initial-seed=0 \\
        --katydid-config=/path/to/katydid_base.yaml \\
        --noise-id=1234 \\
        --max-jobs=8
    # or, in place of --noise-id:
        --noise-paths /path/to/noise_ch0.spec /path/to/noise_ch1.spec
"""

import argparse
import contextvars
import json
import logging
import subprocess
import sys
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from rocks_analysis_pipeline.logging_setup import base_fmt, init_logging
from rocks_analysis_pipeline.stage1_state import task_dir
from rocks_analysis_pipeline.stage2_merge import run_stage2_merge

logger = logging.getLogger(__name__)

# stage1_task's per-task log, written by stage1_task's main() every time
# it runs as a fresh process. Named analogously to
# stage1_steps.SPECSIMS_LOG_FILENAME/KATYDID_LOG_FILENAME, which live
# alongside it in the same task_dir.
STAGE1_TASK_LOG_FILENAME = "stage1_task.log"

# Maps each --keep-<x> flag on this CLI to the matching stage1_task flag
# to pass through unchanged. Both sides use the identical flag name, so
# this is a straight identity map, kept explicit as a table rather than
# blind forwarding, so a flag added to one file doesn't silently start (or
# stop) passing through without a matching, visible entry here.
KEEP_FLAG_PASSTHROUGH: dict[str, str] = {
    "keep_uncompressed_specsims_log": "--keep-uncompressed-specsims-log",
    "keep_mc_truth": "--keep-mc-truth",
    "keep_uncompressed_katydid_log": "--keep-uncompressed-katydid-log",
    "keep_specsims_output": "--keep-specsims-output",
    "keep_katydid_output": "--keep-katydid-output",
    "keep_all": "--keep-all",
}

# Per-thread task label, injected into every log record via
# _TaskContextFilter below.
_task_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("task_ctx", default="-")


class _TaskContextFilter(logging.Filter):
    """Attaches the current thread's task label to every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.task = _task_ctx.get()
        return True


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    # Kebab-case throughout, no short aliases, matching stage1_task's
    # style: both CLIs use the identical flag name for every shared
    # concept.
    arg("--run-name", type=str, required=True, help="run name")
    arg("--runs-dir", type=str, required=True, help="base output directory for runs, matching stage1_task's --runs-dir")
    arg(
        "--yaml-config",
        type=str,
        required=True,
        help="base specsims yaml config, matching stage1_task's --yaml-config",
    )
    arg(
        "--json-config",
        type=str,
        required=True,
        help="base specsims json config (fields_T/traps_A/etc.), matching stage1_task's "
        "--json-config -- also where this driver reads len(fields_T) from, to enumerate "
        "field_index values",
    )
    arg("--num-subruns", type=int, required=True, help="number of subruns, 0..num-subruns-1")
    arg("--initial-seed", type=int, default=0, help="seed for subrun_id=0, matching stage1_task's --initial-seed")
    arg("--katydid-config", type=str, required=True, help="full path to the base katydid yaml config file")

    # Exactly one of --noise-id/--noise-paths, matching stage1_task's
    # mutually exclusive group exactly, passed through unchanged to every
    # task.
    noise_group = par.add_mutually_exclusive_group(required=True)
    noise_group.add_argument("--noise-id", type=int, help="run_id to look up for noise floor -- mutually exclusive with --noise-paths")
    noise_group.add_argument(
        "--noise-paths",
        type=str,
        nargs=2,
        metavar=("CHANNEL_0_PATH", "CHANNEL_1_PATH"),
        help="paths to the two (per-channel) noise .spec(k) files directly -- mutually exclusive with --noise-id",
    )

    arg("--use-ghcss", action="store_true", help="pass --use-ghcss through to every task")

    arg(
        "--max-jobs",
        type=int,
        default=None,
        help="max number of (subrun, field) tasks to run concurrently (default: os.cpu_count())",
    )
    arg("--dry-run", action="store_true", help="print the planned tasks without running them")

    arg("--log-level", type=str, default="INFO", help="root log level -- see logging_setup.init_logging")
    arg(
        "--log-override",
        type=str,
        default=None,
        help="comma-separated logger_name=LEVEL overrides -- see logging_setup.init_logging. Passed "
        "through to every task's --log-override too.",
    )

    # One flag per stage1_task.py --keep-<x> flag, passed straight through
    # to every task -- see KEEP_FLAG_PASSTHROUGH above.
    arg("--keep-uncompressed-specsims-log", dest="keep_uncompressed_specsims_log", action="store_true")
    arg("--keep-mc-truth", dest="keep_mc_truth", action="store_true")
    arg("--keep-uncompressed-katydid-log", dest="keep_uncompressed_katydid_log", action="store_true")
    arg("--keep-specsims-output", dest="keep_specsims_output", action="store_true")
    arg("--keep-katydid-output", dest="keep_katydid_output", action="store_true")
    arg("--keep-all", dest="keep_all", action="store_true")

    return par.parse_args()


def build_jobs(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Enumerates every (subrun_id, field_index) task for this run.

    No upfront config generation is needed to discover field_index values,
    since stage1_task's make_run_specsims renders each task's
    specsims.yaml itself, inside the task -- just len(fields_T) from
    json_config, read once here.
    """
    with open(args.json_config) as f:
        run_params = json.load(f)
    num_fields = len(run_params["fields_T"])

    jobs: list[dict[str, Any]] = []
    for subrun_id in range(args.num_subruns):
        for field_index in range(num_fields):
            jobs.append({"subrun_id": subrun_id, "field_index": field_index})
    return jobs


def build_task_command(args: argparse.Namespace, job: dict[str, Any]) -> list[str]:
    """Builds the stage1_task command line for one task: this process's
    sys.executable running `-m rocks_analysis_pipeline.stage1_task`. Split
    out from _run_one_task so this half -- the part with real, checkable
    logic -- is directly testable without actually invoking anything.
    """
    command = [
        sys.executable,
        "-m",
        "rocks_analysis_pipeline.stage1_task",
        f"--runs-dir={args.runs_dir}",
        f"--run-name={args.run_name}",
        f"--subrun-id={job['subrun_id']}",
        f"--field-index={job['field_index']}",
        f"--yaml-config={args.yaml_config}",
        f"--json-config={args.json_config}",
        f"--initial-seed={args.initial_seed}",
        f"--katydid-config={args.katydid_config}",
    ]

    if args.noise_id is not None:
        command.append(f"--noise-id={args.noise_id}")
    else:
        command += ["--noise-paths", args.noise_paths[0], args.noise_paths[1]]

    if args.use_ghcss:
        command.append("--use-ghcss")

    command.append(f"--log-level={args.log_level}")
    if args.log_override:
        command.append(f"--log-override={args.log_override}")

    for flag_dest, cli_flag in KEEP_FLAG_PASSTHROUGH.items():
        if getattr(args, flag_dest):
            command.append(cli_flag)

    return command


def _run_one_task(args: argparse.Namespace, job: dict[str, Any]) -> Path:
    """Runs a single (subrun, field) task as a fresh subprocess, for crash
    isolation. Returns the task's log path, for the caller's completion
    message.
    """
    task_label = f"subrun {job['subrun_id']} field {job['field_index']}"
    token = _task_ctx.set(task_label)
    try:
        d = task_dir(
            runs_dir=Path(args.runs_dir),
            run_name=args.run_name,
            subrun_id=job["subrun_id"],
            field_index=job["field_index"],
        )
        d.mkdir(parents=True, exist_ok=True)
        log_path = d / STAGE1_TASK_LOG_FILENAME

        command = build_task_command(args, job)
        logger.info("starting")
        # buffering=1 (line-buffered): without it, a log file tailed
        # mid-run can lag far behind or show nothing, even though the task
        # is progressing.
        with open(log_path, "w", buffering=1) as log_file:
            subprocess.run(command, stdout=log_file, stderr=subprocess.STDOUT, check=True)
        logger.info("finished (log: %s)", log_path)
        return log_path
    finally:
        _task_ctx.reset(token)


def main() -> None:
    args = parse_args()
    init_logging(args.log_level, args.log_override)

    # Attach the per-task context filter to every handler init_logging just
    # configured (its basicConfig call), and extend their format to
    # include the injected task label -- see _TaskContextFilter above.
    task_filter = _TaskContextFilter()
    for handler in logging.getLogger().handlers:
        handler.addFilter(task_filter)
        handler.setFormatter(logging.Formatter(f"{base_fmt} [%(task)s]: %(message)s"))

    jobs = build_jobs(args)

    if args.dry_run:
        for job in jobs:
            logger.info("[dry_run] subrun %s field %s", job["subrun_id"], job["field_index"])
        return

    max_jobs = args.max_jobs
    logger.info("Running %d task(s) with max_jobs=%s", len(jobs), max_jobs or "(cpu count)")

    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=max_jobs) as pool:
        futures: dict[Future, dict[str, Any]] = {pool.submit(_run_one_task, args, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            job_label = f"subrun {job['subrun_id']} field {job['field_index']}"
            try:
                future.result()
            except Exception as e:
                failures.append(job_label)
                logger.error("%s: FAILED (%s)", job_label, e)

    if failures:
        logger.error("%d of %d task(s) failed: %s", len(failures), len(jobs), failures)
        sys.exit(1)

    logger.info("All %d task(s) completed successfully.", len(jobs))

    logger.info("starting stage 2 merge")
    run_stage2_merge(runs_dir=Path(args.runs_dir), run_name=args.run_name)
    logger.info("stage 2 merge complete")


if __name__ == "__main__":
    main()
