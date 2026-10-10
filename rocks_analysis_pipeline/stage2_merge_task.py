#!/usr/bin/env python3
"""
Thin CLI driver for stage 2's merge step: a standalone entry point for
running stage 2 against whatever stage-1 output already exists on disk,
rather than only ever in-process right after a stage-1 run.

The --chunk-* flags are all-or-nothing: given together, they scope the
merge to one chunk's job id range and suffix its output filenames; omitted
(the default), this merges every task directory the run has, unsuffixed.

Example:
    bazel run --@pypi//venv=dev //:stage2_merge_task -- \\
        --runs-dir=/path/to/runs \\
        --run-name=test1
"""

import argparse
import logging
from pathlib import Path

from rocks_analysis_pipeline.logging_setup import init_logging
from rocks_analysis_pipeline.stage2_merge import ChunkScope, run_stage2_merge

logger = logging.getLogger(__name__)

_CHUNK_FLAGS = ("chunk_job_id_start", "chunk_job_id_end", "chunk_num_fields")


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("--runs-dir", type=str, required=True, help="base runs directory")
    arg("--run-name", type=str, required=True, help="run name")
    arg(
        "--allow-missing",
        action="store_true",
        help="merge whatever's actually there, skipping (with a warning) any task dir missing bands/dmtracks/"
        "events/points .pb.zst -- default: refuse to write anything at all if anything is missing",
    )

    arg(
        "--chunk-job-id-start",
        type=int,
        default=None,
        help="First job id to merge, inclusive. Only merge this one chunk's task directories "
        "instead of the whole run's. Must be given together with --chunk-job-id-end and "
        "--chunk-num-fields.",
    )
    arg(
        "--chunk-job-id-end",
        type=int,
        default=None,
        help="Last job id to merge, inclusive. Must be given together with --chunk-job-id-start "
        "and --chunk-num-fields.",
    )
    arg(
        "--chunk-num-fields",
        type=int,
        default=None,
        help="Fields per subrun for this run. Needed to work out which subrun and field a job id "
        "belongs to. Must be given together with --chunk-job-id-start and --chunk-job-id-end.",
    )

    arg("--log-level", type=str, default="INFO", help="root log level")
    arg(
        "--log-override",
        type=str,
        default=None,
        help="comma-separated logger_name=LEVEL overrides",
    )

    args = par.parse_args()
    given = [name for name in _CHUNK_FLAGS if getattr(args, name) is not None]
    if given and len(given) != len(_CHUNK_FLAGS):
        missing = ", ".join("--" + n.replace("_", "-") for n in _CHUNK_FLAGS if n not in given)
        par.error(f"missing {missing} -- the --chunk-* flags must all be given together, or not at all")
    return args


def main() -> None:
    args = parse_args()
    init_logging(args.log_level, args.log_override)
    chunk = (
        ChunkScope(
            job_id_start=args.chunk_job_id_start,
            job_id_end=args.chunk_job_id_end,
            num_fields=args.chunk_num_fields,
        )
        if args.chunk_job_id_start is not None
        else None
    )
    run_stage2_merge(
        runs_dir=Path(args.runs_dir), run_name=args.run_name, allow_missing=args.allow_missing, chunk=chunk
    )


if __name__ == "__main__":
    main()
