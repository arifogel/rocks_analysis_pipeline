#!/usr/bin/env python3
"""
wulf orchestration driver for this project's simulate-and-analyze pipeline:
local_ssa.py's direct sibling, submitting Slurm jobs instead of running
local subprocesses.

Map: every (subrun, field) simulation+analysis task for a run, run in
parallel on wulf's own compute nodes, chunked into one or more Slurm job
arrays (see --chunk-size). Reduce: one job per chunk, each dependent only
on its own chunk's map tasks finishing (not necessarily succeeding -- see
--allow-missing), merging that chunk's own outputs into one of this run's
per-chunk results -- never more than one chunk's worth of tasks in memory
at once, bounding a reduce job's own memory footprint regardless of how
large the run is.

Input: a run name, a base specsims yaml/json config, a katydid config,
and a noise reference (--noise-id or --noise-paths) -- see --help for the
full set.

Output: four files per chunk under runs_dir/run_name/ -- bands-<n>.pb.zst,
dmtracks-<n>.pb.zst, events-<n>.pb.zst, points-<n>.pb.zst, <n> the chunk's
0-based index -- see stage2_merge.py's own ChunkScope/_chunked_filename
doc comments. The
unsuffixed bands.pb.zst/etc. local_ssa.py itself produces is a single-chunk
special case of the same four files, not something this script also writes.

Side effects: one map sbatch invocation per chunk, each immediately followed by that
chunk's own reduce sbatch invocation. Map: every (subrun, field) pair, one Slurm task
each, submitted as one or more Slurm job arrays -- chunked at --chunk-size tasks per
array, since a single array job can only hold as many tasks as the cluster's MaxArraySize
allows (see --chunk-size's help for how to find that limit). Each task writes its own log
under runs_dir/run_name/subrun_*/field_*/, matching where a locally-run task would.
Reduce: one single, non-array job per chunk, submitted with
--dependency=afterany:<that chunk's own map array id> -- Slurm starts it once every task
in that chunk has finished, whether or not each one succeeded (afterok would instead
block this job forever the moment any single array task fails, defeating
stage2_merge_task's --allow-missing). Every job's own sbatch stdout/stderr capture (a
fallback for whatever each doesn't already log itself, e.g. a crash before its own
logging starts) goes under runs_dir/run_name/slurm_logs/.

Execution environment: run this script on cenpa-wulf's own head node; it
does no simulation, analysis, or merge work itself, only submits jobs
that do.

Each map task gets a single flat $SLURM_ARRAY_TASK_ID from Slurm,
translated into (subrun_id, field_index) via stage1_task.py's own
--job-id/--num-fields. The reduce job runs stage2_merge_task.py. (This
project's own vocabulary for these two phases, used throughout the rest
of the codebase -- e.g. stage1_task.py, stage2_merge.py -- is "stage
1"/"stage 2", should further reading lead there.)

Run this script with --help for its own full flag reference.
"""

import argparse
import json
import logging
import shlex
from pathlib import Path
from typing import Any

from python.runfiles import runfiles
from rocks_analysis_pipeline.logging_setup import init_logging
from rocks_analysis_pipeline.rocks_utility import sbatch_job

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

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
    arg("--num-subruns", type=int, required=True, help="number of subruns, 0..num-subruns-1")
    arg("--initial-seed", type=int, default=0, help="seed for subrun_id=0, matching stage1_task.py's own --initial-seed")
    arg("--katydid-config", type=str, required=True, help="full path to the base katydid yaml config file")

    noise_group = par.add_mutually_exclusive_group(required=True)
    noise_group.add_argument("--noise-id", type=int, help="run_id to look up for noise floor -- mutually exclusive with --noise-paths")
    noise_group.add_argument(
        "--noise-paths",
        type=str,
        nargs=2,
        metavar=("CHANNEL_0_PATH", "CHANNEL_1_PATH"),
        help="paths to the two (per-channel) noise .spec(k) files directly -- mutually exclusive with --noise-id",
    )

    arg("--use-ghcss", action="store_true", help="pass --use-ghcss through to every simulation+analysis task")

    # Job-control surface matches sbatch_ssa_katydid.py/sbatch_spec_sims.py/
    # sbatch_ssa_post_processing.py's own union exactly: --tlim is the only
    # job-control flag any of them expose, each with its own default rather
    # than requiring it. cpus-per-task/mem/concurrency limits have no
    # precedent in any of them and aren't included here either.
    arg(
        "--tlim",
        type=str,
        default="48:00:00",
        help="sbatch --time (HH:MM:SS) for each simulation+analysis task (applies per task, the same "
        "as any other Slurm array: not split across the array as a whole)",
    )
    arg("--merge-tlim", type=str, default="12:00:00", help="sbatch --time (HH:MM:SS) for the merge job")

    arg(
        "--chunk-size",
        type=int,
        default=1000,
        help="max tasks per chunk -- the full (subrun, field) task list is split into this many "
        "tasks per chunk, one map Slurm job array and one dependent reduce job per chunk. Bounded "
        "above by the cluster's MaxArraySize (`scontrol show config | grep MaxArraySize`), since a "
        "chunk's map tasks are submitted as a single array job; default is comfortably under "
        "Slurm's stock 1001 default -- lower this if this cluster's MaxArraySize is smaller. Also "
        "bounds each reduce job's own memory footprint, since it only ever merges one chunk's "
        "worth of tasks at a time (see this module's own doc comment)",
    )

    arg("--allow-missing", action="store_true", help="pass --allow-missing through to the merge job")

    arg("--dry-run", action="store_true", help="print the map/reduce commands that would be submitted, without calling sbatch")

    arg("--log-level", type=str, default="INFO", help="root log level -- see logging_setup.init_logging")
    arg(
        "--log-override",
        type=str,
        default=None,
        help="comma-separated logger_name=LEVEL overrides -- see logging_setup.init_logging. Passed "
        "through to both submitted jobs' own --log-override too.",
    )

    arg("--keep-uncompressed-specsims-log", dest="keep_uncompressed_specsims_log", action="store_true")
    arg("--keep-mc-truth", dest="keep_mc_truth", action="store_true")
    arg("--keep-uncompressed-katydid-log", dest="keep_uncompressed_katydid_log", action="store_true")
    arg("--keep-specsims-output", dest="keep_specsims_output", action="store_true")
    arg("--keep-katydid-output", dest="keep_katydid_output", action="store_true")
    arg("--keep-all", dest="keep_all", action="store_true")

    return par.parse_args()


def resolve_launcher_path() -> str:
    """Resolves tools/run_via_warmed_runfiles.sh's own real path via bazel
    runfiles.
    """
    r = runfiles.Create()
    return r.Rlocation("_main/tools/run_via_warmed_runfiles.sh")


def resolve_runfiles_dir(launcher_path: str) -> str:
    """release_venv_warmed/'s path, computed from launcher_path (run_via_warmed_runfiles.sh's
    resolved path).

    Passed to run_via_warmed_runfiles.sh explicitly in every submitted job: wulf runs this
    against an extracted release tarball from the head node, not a bazel checkout, so
    run_via_warmed_runfiles.sh's bazel-bin-relative fallback has nothing to find there.
    """
    return str(Path(launcher_path).parent.parent.parent)


def build_jobs_and_num_fields(args: argparse.Namespace) -> tuple[list[dict[str, Any]], int]:
    """Enumerates every (subrun_id, field_index) task for this run, and
    returns num_fields alongside it: stage1_task.py's own --job-id/
    --num-fields derivation (job_id // num_fields, job_id % num_fields)
    needs the same num_fields value this enumeration itself used, for
    $SLURM_ARRAY_TASK_ID to map back to the same (subrun_id, field_index)
    pairs in the same order.
    """
    with open(args.json_config) as f:
        run_params = json.load(f)
    num_fields = len(run_params["fields_T"])

    jobs: list[dict[str, Any]] = []
    for subrun_id in range(args.num_subruns):
        for field_index in range(num_fields):
            jobs.append({"subrun_id": subrun_id, "field_index": field_index})
    return jobs, num_fields


def build_job_chunks(num_jobs: int, chunk_size_limit: int) -> list[tuple[int, int]]:
    """Splits the full, global 0..num_jobs-1 task-id range into consecutive chunks of at most
    chunk_size_limit each, one per Slurm job array -- see --chunk-size's help for why a
    single array can't just hold all of num_jobs.

    Returns a list of (offset, chunk_size) pairs, in order: chunk i covers global task ids
    offset..offset+chunk_size-1, submitted as that chunk's array job with local indices
    0..chunk_size-1. offset is each chunk's global starting position, to be reapplied wherever
    a chunk's local $SLURM_ARRAY_TASK_ID needs mapping back to its real, global task id, and
    wherever its reduce job needs mapping back to its own job id range.
    """
    chunks: list[tuple[int, int]] = []
    offset = 0
    while offset < num_jobs:
        chunk_size = min(chunk_size_limit, num_jobs - offset)
        chunks.append((offset, chunk_size))
        offset += chunk_size
    return chunks


def build_map_command(
    launcher_path: str, runfiles_dir: str, args: argparse.Namespace, num_fields: int, job_id_offset: int
) -> str:
    """Builds the shell command each map task runs.

    job_id_offset shifts $SLURM_ARRAY_TASK_ID -- which restarts at 0 in every chunk's array
    job -- back to that task's real, global position in the full (subrun, field) task list.
    """
    parts = [
        f"RUNFILES_DIR={shlex.quote(runfiles_dir)}",
        shlex.quote(launcher_path),
        "release_venv",
        "stage1_task",
        shlex.quote(f"--runs-dir={args.runs_dir}"),
        shlex.quote(f"--run-name={args.run_name}"),
        # Deliberately unquoted, so the shell on the compute node expands it at runtime --
        # shlex.quote would suppress that expansion entirely. The arithmetic expansion adds
        # job_id_offset even when it's 0, rather than special-casing the first chunk, so every
        # chunk's command is built the same way.
        f"--job-id=$(($SLURM_ARRAY_TASK_ID + {job_id_offset}))",
        shlex.quote(f"--num-fields={num_fields}"),
        shlex.quote(f"--yaml-config={args.yaml_config}"),
        shlex.quote(f"--json-config={args.json_config}"),
        shlex.quote(f"--initial-seed={args.initial_seed}"),
        shlex.quote(f"--katydid-config={args.katydid_config}"),
    ]

    if args.noise_id is not None:
        parts.append(shlex.quote(f"--noise-id={args.noise_id}"))
    else:
        parts += ["--noise-paths", shlex.quote(args.noise_paths[0]), shlex.quote(args.noise_paths[1])]

    if args.use_ghcss:
        parts.append("--use-ghcss")

    parts.append(shlex.quote(f"--log-level={args.log_level}"))
    if args.log_override:
        parts.append(shlex.quote(f"--log-override={args.log_override}"))

    if args.keep_uncompressed_specsims_log:
        parts.append("--keep-uncompressed-specsims-log")
    if args.keep_mc_truth:
        parts.append("--keep-mc-truth")
    if args.keep_uncompressed_katydid_log:
        parts.append("--keep-uncompressed-katydid-log")
    if args.keep_specsims_output:
        parts.append("--keep-specsims-output")
    if args.keep_katydid_output:
        parts.append("--keep-katydid-output")
    if args.keep_all:
        parts.append("--keep-all")

    return " ".join(parts)


def build_reduce_command(
    launcher_path: str,
    runfiles_dir: str,
    args: argparse.Namespace,
    chunk_index: int,
) -> str:
    """Builds the shell command one chunk's reduce job runs -- --chunk-index
    (plus --chunk-size, matching this run's own --chunk-size) scopes it to
    this chunk's own slice of the run's task directories and filename
    suffix (see stage2_merge_task.py's own --help).
    """
    parts = [
        f"RUNFILES_DIR={shlex.quote(runfiles_dir)}",
        shlex.quote(launcher_path),
        "release_venv",
        "stage2_merge_task",
        shlex.quote(f"--runs-dir={args.runs_dir}"),
        shlex.quote(f"--run-name={args.run_name}"),
        shlex.quote(f"--chunk-index={chunk_index}"),
        shlex.quote(f"--chunk-size={args.chunk_size}"),
    ]
    if args.allow_missing:
        parts.append("--allow-missing")
    parts.append(shlex.quote(f"--log-level={args.log_level}"))
    if args.log_override:
        parts.append(shlex.quote(f"--log-override={args.log_override}"))
    return " ".join(parts)


def submit_map_chunk(
    launcher_path: str,
    runfiles_dir: str,
    args: argparse.Namespace,
    num_fields: int,
    chunk_index: int,
    offset: int,
    chunk_size: int,
) -> str:
    """Submits one chunk's map tasks as one Slurm job array, returning its primary job id
    (sbatch_job's --parsable).
    """
    slurm_log_dir = Path(args.runs_dir) / args.run_name / "slurm_logs"
    slurm_log_dir.mkdir(parents=True, exist_ok=True)
    # %A/%a: Slurm's own array-job/array-task-id placeholders, substituted by
    # Slurm itself per task. This is sbatch's own stdout/stderr capture, a
    # fallback for whatever stage1_task itself doesn't already log (e.g. a
    # crash before its own init_logging even runs) -- stage1_task.py's own,
    # more detailed per-task log still lands at task_dir/stage1_task.log as
    # usual.
    log_path = slurm_log_dir / f"map_chunk{chunk_index}_%A_%a.log"

    cmd = build_map_command(launcher_path, runfiles_dir, args, num_fields, job_id_offset=offset)
    proc = sbatch_job(
        cmd=cmd,
        job_name=f"{args.run_name}_map_chunk{chunk_index}",
        tlim=args.tlim,
        log_path=log_path,
        array=chunk_size,
    )
    return proc.stdout.strip()


def submit_reduce_chunk(
    launcher_path: str,
    runfiles_dir: str,
    args: argparse.Namespace,
    chunk_index: int,
    map_job_id: str,
) -> str:
    """Submits one chunk's reduce job, with --dependency=afterany:<that chunk's own map job id>."""
    slurm_log_dir = Path(args.runs_dir) / args.run_name / "slurm_logs"
    slurm_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = slurm_log_dir / f"reduce_chunk{chunk_index}_%j.log"

    cmd = build_reduce_command(launcher_path, runfiles_dir, args, chunk_index)
    proc = sbatch_job(
        cmd=cmd,
        job_name=f"{args.run_name}_reduce_chunk{chunk_index}",
        tlim=args.merge_tlim,
        log_path=log_path,
        dependency=f"afterany:{map_job_id}",
    )
    return proc.stdout.strip()


def main() -> None:
    args = parse_args()
    init_logging(args.log_level, args.log_override)

    jobs, num_fields = build_jobs_and_num_fields(args)
    logger.info("Enumerated %d task(s) (%d subrun(s) x %d field(s))", len(jobs), args.num_subruns, num_fields)

    launcher_path = resolve_launcher_path()
    runfiles_dir = resolve_runfiles_dir(launcher_path)

    chunks = build_job_chunks(len(jobs), args.chunk_size)
    num_chunks = len(chunks)
    logger.info("Split into %d chunk(s) of up to %d task(s) each (--chunk-size)", num_chunks, args.chunk_size)

    if args.dry_run:
        for chunk_index, (offset, chunk_size) in enumerate(chunks):
            logger.info(
                "[dry_run] chunk %d map command (%d tasks, offset %d):\n%s",
                chunk_index,
                chunk_size,
                offset,
                build_map_command(launcher_path, runfiles_dir, args, num_fields, job_id_offset=offset),
            )
            logger.info(
                "[dry_run] chunk %d reduce command:\n%s",
                chunk_index,
                build_reduce_command(launcher_path, runfiles_dir, args, chunk_index),
            )
        return

    for chunk_index, (offset, chunk_size) in enumerate(chunks):
        map_job_id = submit_map_chunk(launcher_path, runfiles_dir, args, num_fields, chunk_index, offset, chunk_size)
        logger.info("submitted map chunk %d: job %s (%d tasks)", chunk_index, map_job_id, chunk_size)

        reduce_job_id = submit_reduce_chunk(launcher_path, runfiles_dir, args, chunk_index, map_job_id)
        logger.info(
            "submitted reduce chunk %d: job %s (runs after map chunk %d's job %s completes)",
            chunk_index,
            reduce_job_id,
            chunk_index,
            map_job_id,
        )


if __name__ == "__main__":
    main()
