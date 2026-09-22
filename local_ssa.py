#!/usr/bin/env python3
"""
Local, parallel orchestration driver for this project's ssa (spec-sims and
analysis) pipeline -- currently stage 1 only (fans out stage1_task across
every (subrun_id, field_index) task for a run, via a ThreadPoolExecutor,
not a ProcessPoolExecutor -- see the reasoning below); stage 2 (the merge
step, not yet designed) is meant to join this same orchestrator once it
exists, rather than get its own separate driver.

Crash isolation is achieved by stage1_task itself running as its own fresh
subprocess, once per task -- not by this orchestrator's own worker being a
separate process. A hard crash (segfault, OOM-kill) inside one task's own
Simulation.run_full() or Katydid invocation only ever kills that task's own
subprocess, never this orchestrator or any other task's own worker. Given
that, a ThreadPoolExecutor is the right tool here, not a ProcessPoolExecutor:
each worker thread is mostly just waiting on a subprocess (I/O-bound, not
CPU-bound in this process), matching cresproc/model.py's own reasoning for
using threads over processes for comparable I/O-bound work.

Each task's own subprocess is this process's own sys.executable, running
`-m stage1_task` -- not stage1_task's own bazel-generated py_binary
launcher, and not a resolved path to stage1_task.py's own source file.
Two earlier, wrong approaches got as far as being committed before this
one, both real bugs, not refinements:

First: invoking stage1_task's own py_binary launcher directly, once per
task. That launcher creates and manages its own separate, per-binary venv
every time it runs, and a fresh runfiles tree gets created on every single
invocation -- confirmed directly against a real crash, not assumed -- so
many tasks' own launchers racing to set up that same venv concurrently
produced a real, reproducible crash (a different failure message each
time, depending on exactly how two invocations collided).

Second: keeping sys.executable, but resolving stage1_task.py's own source
file via runfiles.Rlocation and warming up stage1_task's own venv exactly
once before any tasks start, then invoking that venv's own python3
directly against the resolved source path. This failed for a more basic
reason: a source file's own resolved runfile is very often just a symlink
straight back into the actual checkout, with no reliable "runfiles root"
to derive stage1_task's own venv path by walking up from it -- confirmed
directly against a real crash (a RuntimeError from a nonsense derived
path), not assumed.

The actual fix addresses the real, underlying cause directly instead of
working around it: this process's own venv didn't have stage1_task's own
package deps (numpy, uproot, he6-cres-spec-sims, etc.) only because
BUILD.bazel's own local_ssa target depended on :stage1_task as a data
dependency (bundling its files) rather than :stage1_task_lib as a real
deps dependency (pulling in its own package deps too) -- each
aspect_rules_py venv is built from its own target's own deps, not from
what a data dependency happens to bundle in. With that fixed, this
process's own venv genuinely has everything stage1_task itself needs, so
sys.executable is exactly the right interpreter after all, and `-m
stage1_task` needs no resolved file path at all -- Python's own import
machinery finds it directly, sidestepping runfiles-path-guessing
entirely. This also means no separate venv, and no warm-up step (in-band
or manual), is needed anywhere: there is only ever the one venv, this
process's own, set up once by bazel's own launcher before any tasks run.

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

Flags are kebab-case, matching stage1_task.py's own convention -- and, for
every flag this shares a concept with (--runs-dir, --run-name,
--yaml-config, --json-config, --initial-seed, --katydid-config,
--noise-id/--noise-paths, --use-ghcss, --log-level, --log-override, every
--keep-<x>), the name here is identical to stage1_task.py's own, not just
similarly named -- so a value copied from one CLI's own --help works
unchanged on the other. Deliberately different from local_spec_sims.py/
local_ssa_katydid.py's own snake_case convention for this same kind of
orchestration-layer file.

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

from logging_setup import base_fmt, init_logging
from stage1_state import task_dir

logger = logging.getLogger(__name__)

# stage1_task's own per-task log, written by stage1_task.py's own main()
# every time it runs (a fresh process, so it calls init_logging itself --
# see this module's own docstring). Named analogously to
# stage1_steps.SPECSIMS_LOG_FILENAME/KATYDID_LOG_FILENAME, which live
# alongside it in the same task_dir.
STAGE1_TASK_LOG_FILENAME = "stage1_task.log"

# Maps each --keep-<x> flag on *this* CLI to the matching stage1_task.py
# flag to pass through unchanged -- both sides use the identical flag name
# (see this module's own doc comment on why), so this is a straight
# identity map; kept explicit anyway, matching stage1_task.py's own
# KEEP_FLAG_STEPS pattern of a table rather than blind forwarding, so a
# flag added to one file doesn't silently start (or stop) passing through
# without a matching, visible entry here.
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


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    # Kebab-case throughout, no short aliases -- matching stage1_task.py's
    # own style exactly (see this module's own doc comment on why), not
    # local_spec_sims.py/local_ssa_katydid.py's own snake_case-plus-short-
    # alias convention.
    arg("--run-name", type=str, required=True, help="run name")
    arg("--runs-dir", type=str, required=True, help="base output directory for runs, matching stage1_task.py's own --runs-dir")
    arg("--yaml-config", type=str, required=True, help="base specsims yaml config, matching stage1_task.py's own --yaml-config")
    arg(
        "--json-config",
        type=str,
        required=True,
        help="base specsims json config (fields_T/traps_A/etc.), matching stage1_task.py's own "
        "--json-config -- also where this driver reads len(fields_T) from, to enumerate "
        "field_index values",
    )
    arg("--num-subruns", type=int, default=1, help="number of subruns, 0..num-subruns-1")
    arg("--initial-seed", type=int, default=0, help="seed for subrun_id=0, matching stage1_task.py's own --initial-seed")
    arg("--katydid-config", type=str, required=True, help="full path to the base katydid yaml config file")

    # Exactly one of --noise-id/--noise-paths, matching stage1_task.py's
    # own mutually exclusive group exactly (see that file's own doc
    # comment) -- passed through unchanged to every task.
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


def build_task_command(args: argparse.Namespace, job: dict[str, Any]) -> list[str]:
    """Builds the stage1_task command line for one task: this process's own
    sys.executable running `-m stage1_task` -- see this module's own
    docstring for why this, and not stage1_task's own py_binary launcher
    or a resolved path to its source file, is correct. Split out from
    _run_one_task so this half -- the part with real, checkable logic --
    is directly testable without actually invoking anything.
    """
    command = [
        sys.executable,
        "-m",
        "stage1_task",
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
    """Runs a single (subrun, field) task as its own fresh subprocess --
    see this module's own docstring for why a subprocess (crash isolation)
    and why a ThreadPoolExecutor rather than a ProcessPoolExecutor is what
    fans these out. Returns the task's own log path, for the caller's own
    completion message.
    """
    task_label = f"subrun {job['subrun_id']} field {job['field_index']}"
    token = _task_ctx.set(task_label)
    try:
        d = task_dir(Path(args.runs_dir), args.run_name, job["subrun_id"], job["field_index"])
        d.mkdir(parents=True, exist_ok=True)
        log_path = d / STAGE1_TASK_LOG_FILENAME

        command = build_task_command(args, job)
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


if __name__ == "__main__":
    main()
