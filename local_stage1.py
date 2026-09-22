#!/usr/bin/env python3
"""
Local, parallel driver for stage 1: fans out stage1_task.py across every
(subrun_id, field_index) task for a run, via a ThreadPoolExecutor -- not a
ProcessPoolExecutor (see the reasoning below).

Crash isolation is achieved by stage1_task itself running as its own fresh
subprocess, once per task -- not by this orchestrator's own worker being a
separate process. A hard crash (segfault, OOM-kill) inside one task's own
Simulation.run_full() or Katydid invocation only ever kills that task's own
subprocess, never this orchestrator or any other task's own worker. Given
that, a ThreadPoolExecutor is the right tool here, not a ProcessPoolExecutor:
each worker thread is mostly just waiting on a subprocess (I/O-bound, not
CPU-bound in this process), matching cresproc/model.py's own reasoning for
using threads over processes for comparable I/O-bound work.

Because multiple worker threads share this one process's root logger, each
log record is tagged with which task its own thread is currently on (via a
contextvars.ContextVar + logging.Filter -- the same mechanism
cresproc/model.py's own SubprocessContextFilter uses for its own
process-pool workers, adapted here for threads instead), otherwise
concurrent tasks' own "starting"/"finished" messages would be
indistinguishable on this orchestrator's own shared console. This is this
orchestrator's own, brief per-task status only; stage1_task's own full
per-task detail (the katydid command, row counts, etc.) already goes to its
own stage1_task.log, written by stage1_task's own main() every time it runs
as a fresh subprocess (init_logging is called there, same as here, since a
subprocess doesn't inherit this process's own logging config) -- nothing
about that needs reinitializing or redirecting from here.

Example:
    bazel run --@pypi//venv=dev //:local_stage1 -- \\
        --run_name=test1 \\
        --runs_base_dir=/path/to/runs \\
        --yaml_config=/path/to/base.yaml \\
        --json_config=/path/to/base.json \\
        --num_subruns=25 \\
        --initial_seed=0 \\
        --katydid_config=/path/to/katydid_base.yaml \\
        --noise_id=1234 \\
        --max-jobs=8
    # or, in place of --noise_id:
        --noise_paths /path/to/noise_ch0.spec /path/to/noise_ch1.spec
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

from python.runfiles import runfiles

from logging_setup import base_fmt, init_logging
from stage1_state import task_dir

logger = logging.getLogger(__name__)

# Matches this repo's own module(name = ...) in MODULE.bazel -- the
# runfiles-relative prefix for a target defined in this same workspace, not
# an external dependency (unlike e.g. stage1_steps.py's own
# SPECSIMS_RLOCATION/KATYDID_RLOCATION, which use "<module>+/..." for
# external deps pulled in via bazel_dep).
STAGE1_TASK_RLOCATION = "rocks-analysis-pipeline/stage1_task"

# stage1_task's own per-task log, written by stage1_task.py's own main()
# every time it runs (a fresh process, so it calls init_logging itself --
# see this module's own docstring). Named analogously to
# stage1_steps.SPECSIMS_LOG_FILENAME/KATYDID_LOG_FILENAME, which live
# alongside it in the same task_dir.
STAGE1_TASK_LOG_FILENAME = "stage1_task.log"

# Maps each --keep-<x> flag on *this* CLI to the matching stage1_task.py
# flag to pass through unchanged -- kept as an explicit map (not just
# forwarding args verbatim) so this file's own flags stay independently
# named/documented, matching stage1_task.py's own KEEP_FLAG_STEPS pattern
# of an explicit table rather than positional pass-through.
KEEP_FLAG_PASSTHROUGH: dict[str, str] = {
    "keep_uncompressed_specsims_log": "--keep-uncompressed-specsims-log",
    "keep_mc_truth": "--keep-mc-truth",
    "keep_uncompressed_katydid_log": "--keep-uncompressed-katydid-log",
    "keep_specsims_output": "--keep-specsims-output",
    "keep_katydid_output": "--keep-katydid-output",
    "keep_all": "--keep-all",
}

# Per-thread task label, injected into every log record via
# _TaskContextFilter below -- see this module's own docstring.
_task_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("task_ctx", default="-")


class _TaskContextFilter(logging.Filter):
    """Attaches the current thread's own task label to every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.task = _task_ctx.get()
        return True


def resolve_stage1_task_path() -> str:
    """Resolves stage1_task's own bazel-generated launcher via runfiles.
    That launcher is self-locating -- it finds its own stage1_task.runfiles/
    tree sitting next to itself on disk, regardless of who spawns it or
    what this (calling) process's own runfiles tree looks like -- so no
    extra environment setup is needed for the subprocess this gets used to
    start, unlike a raw (non-bazel-launcher) script would need.
    """
    r = runfiles.Create()
    path = r.Rlocation(STAGE1_TASK_RLOCATION)
    if path is None or not Path(path).is_file():
        raise RuntimeError(
            f"Could not resolve stage1_task via runfiles at "
            f"'{STAGE1_TASK_RLOCATION}' (got: {path}). If this repo's own "
            f"module name (MODULE.bazel's own module(name=...)) or "
            f"stage1_task's own BUILD.bazel target name has changed, "
            f"update STAGE1_TASK_RLOCATION at the top of this file."
        )
    return path


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    # snake_case, matching local_spec_sims.py/local_ssa_katydid.py's own
    # convention for this orchestration layer -- deliberately different
    # from stage1_task.py's own kebab-case (see that file's own doc
    # comment on why), since this file is the same kind of thing as those
    # two, not a single-task entry point.
    arg("-r", "--run_name", type=str, required=True, help="run name")
    arg("-rb", "--runs_base_dir", type=str, required=True, help="base output directory for runs")
    arg("-y", "--yaml_config", type=str, required=True, help="base specsims yaml config, matching local_spec_sims.py's own --yaml_config")
    arg(
        "-j",
        "--json_config",
        type=str,
        required=True,
        help="base specsims json config (fields_T/traps_A/etc.) -- also where this driver reads "
        "len(fields_T) from, to enumerate field_index values",
    )
    arg("-n", "--num_subruns", type=int, default=1, help="number of subruns, 0..num_subruns-1")
    arg("-s0", "--initial_seed", type=int, default=0, help="seed for subrun_id=0, matching stage1_task.py's own --initial-seed")
    arg("-kc", "--katydid_config", type=str, required=True, help="full path to the base katydid yaml config file")

    # Exactly one of --noise_id/--noise_paths, matching stage1_task.py's
    # own mutually exclusive group exactly (see that file's own doc
    # comment) -- passed through unchanged to every task.
    noise_group = par.add_mutually_exclusive_group(required=True)
    noise_group.add_argument("-nid", "--noise_id", type=int, help="run_id to look up for noise floor -- mutually exclusive with --noise_paths")
    noise_group.add_argument(
        "-np",
        "--noise_paths",
        type=str,
        nargs=2,
        metavar=("CHANNEL_0_PATH", "CHANNEL_1_PATH"),
        help="paths to the two (per-channel) noise .spec(k) files directly -- mutually exclusive with --noise_id",
    )

    arg("--use_ghcss", action="store_true", help="pass --use-ghcss through to every task")

    arg(
        "--max-jobs",
        dest="max_jobs",
        type=int,
        default=None,
        help="max number of (subrun, field) tasks to run concurrently (default: os.cpu_count())",
    )
    arg("-d", "--dry_run", action="store_true", help="print the planned tasks without running them")

    arg("--log-level", dest="log_level", type=str, default="INFO", help="root log level -- see logging_setup.init_logging")
    arg(
        "--log-override",
        dest="log_override",
        type=str,
        default=None,
        help="comma-separated logger_name=LEVEL overrides -- see logging_setup.init_logging. Passed "
        "through to every task's own --log-override too.",
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

    Unlike local_spec_sims.py's own build_jobs, this needs no upfront
    config generation to discover field_index values (stage1_task.py's own
    make_run_specsims renders each task's own specsims.yaml itself, inside
    the task) -- just len(fields_T) from json_config, read once here.
    """
    with open(args.json_config) as f:
        run_params = json.load(f)
    num_fields = len(run_params["fields_T"])

    jobs: list[dict[str, Any]] = []
    for subrun_id in range(args.num_subruns):
        for field_index in range(num_fields):
            jobs.append({"subrun_id": subrun_id, "field_index": field_index})
    return jobs


def build_task_command(stage1_task_path: str, args: argparse.Namespace, job: dict[str, Any]) -> list[str]:
    """Builds the stage1_task command line for one task. Split out from
    _run_one_task so this half -- the part with real, checkable logic --
    is directly testable without needing a real stage1_task_path or to
    actually invoke it.
    """
    command = [
        stage1_task_path,
        f"--runs-dir={args.runs_base_dir}",
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


def _run_one_task(stage1_task_path: str, args: argparse.Namespace, job: dict[str, Any]) -> Path:
    """Runs a single (subrun, field) task as its own fresh subprocess --
    see this module's own docstring for why a subprocess (crash isolation)
    and why a ThreadPoolExecutor rather than a ProcessPoolExecutor is what
    fans these out. Returns the task's own log path, for the caller's own
    completion message.
    """
    task_label = f"subrun {job['subrun_id']} field {job['field_index']}"
    token = _task_ctx.set(task_label)
    try:
        d = task_dir(Path(args.runs_base_dir), args.run_name, job["subrun_id"], job["field_index"])
        d.mkdir(parents=True, exist_ok=True)
        log_path = d / STAGE1_TASK_LOG_FILENAME

        command = build_task_command(stage1_task_path, args, job)
        logger.info("starting")
        # buffering=1 (line-buffered): same reasoning as local_spec_sims.py's
        # own _run_one_job -- without it, a log file tailed mid-run can lag
        # far behind or show nothing, even though the task is progressing.
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
    # configured (its own basicConfig call), and extend their format to
    # include the injected task label -- see this module's own docstring
    # and _TaskContextFilter above.
    task_filter = _TaskContextFilter()
    for handler in logging.getLogger().handlers:
        handler.addFilter(task_filter)
        handler.setFormatter(logging.Formatter(f"{base_fmt} [%(task)s]: %(message)s"))

    jobs = build_jobs(args)

    if args.dry_run:
        for job in jobs:
            logger.info("[dry_run] subrun %s field %s", job["subrun_id"], job["field_index"])
        return

    stage1_task_path = resolve_stage1_task_path()

    max_jobs = args.max_jobs
    logger.info("Running %d task(s) with max_jobs=%s", len(jobs), max_jobs or "(cpu count)")

    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=max_jobs) as pool:
        futures: dict[Future, dict[str, Any]] = {
            pool.submit(_run_one_task, stage1_task_path, args, job): job for job in jobs
        }
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


if __name__ == "__main__":
    main()
