#!/usr/bin/env python3
"""
Thin CLI driver for stage 2's own merge step (see stage2_merge.py's own
module doc comment for the real logic and reasoning) -- exists as its own,
separate entry point for the wulf case: stage 1 orchestrated separately
from local_ssa (see local_ssa.py's own module doc comment on why), needing
stage 2 run as its own, standalone step against whatever stage-1 output
already exists on disk, rather than only ever in-process from local_ssa's
own main().

Flags match local_ssa.py's own --runs-dir/--run-name exactly, for the same
reason local_ssa.py's own flags match stage1_task.py's -- a value copied
from one CLI's own --help works unchanged on the other.

The --chunk-* flags are all-or-nothing: given together, they scope the
merge to one chunk's own job id range and suffix its output filenames --
see stage2_merge.py's own ChunkScope doc comment. wulf_ssa.py's per-chunk
reduce job is the only caller that passes them; omitted (the default),
this merges every task directory the run has, unsuffixed.

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

    arg("--runs-dir", type=str, required=True, help="base runs directory, matching local_ssa.py's own --runs-dir")
    arg("--run-name", type=str, required=True, help="run name, matching local_ssa.py's own --run-name")
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
        help="inclusive start of this chunk's job id range -- required alongside the other "
        "--chunk-* flags, to merge only this chunk's own task directories",
    )
    arg(
        "--chunk-job-id-end",
        type=int,
        default=None,
        help="inclusive end of this chunk's job id range -- required alongside the other "
        "--chunk-* flags",
    )
    arg(
        "--chunk-num-fields",
        type=int,
        default=None,
        help="num_fields value job ids were derived from (matching stage1_task.py's own --job-id/"
        "--num-fields) -- required alongside the other --chunk-* flags",
    )

    arg("--log-level", type=str, default="INFO", help="root log level -- see logging_setup.init_logging")
    arg(
        "--log-override",
        type=str,
        default=None,
        help="comma-separated logger_name=LEVEL overrides -- see logging_setup.init_logging",
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
    run_stage2_merge(Path(args.runs_dir), args.run_name, allow_missing=args.allow_missing, chunk=chunk)


if __name__ == "__main__":
    main()
