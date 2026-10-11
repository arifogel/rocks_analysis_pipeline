"""Stage 2: archives stage-1 task output into per-run tar files --
runs_dir/run_name/{bands,dmtracks,events,points}.tar, sibling to that run's
subrun_*/ directories.

Each tar holds one stage-1 file per task, copied byte for byte, as
subrun_<id>/field_<index>/<filename>. Every member is a complete
BandList/DMTrackList/EventList/PointList carrying its own TaskIdentity.

run_stage2_merge archives every task directory the run has into one
unsuffixed set of tars. Passing a ChunkScope archives only that chunk's task
directories, into filenames with _<job id start>-<job id end> inserted
before the extension. Passing types archives only those types. Memory use
is constant in the number of tasks.

slew_times is excluded: identical across every task of a run (same DAQ
config, same simulated acquisition timing).

Strict: if any task directory is missing a requested type's .pb.zst,
run_stage2_merge raises before writing anything.

Each type's archive is a checkpointed step in runs_dir/run_name/
(.checkpoint-merge-<type>, with -<job id start>-<job id end> appended for a
chunk). Rerunning skips the types already archived.

Runs in-process.
"""

import logging
import tarfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from rocks_analysis_pipeline.checkpoints import is_checkpointed, run_checkpointed
from rocks_analysis_pipeline.proto_outputs import ARCHIVE_TYPES, OUTPUT_BY_NAME
from rocks_analysis_pipeline.stage1_state import task_dir

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True, slots=True)
class ChunkScope:
    """One reduce chunk's job id range: job_id_start through job_id_end,
    both inclusive, plus the num_fields value those job ids are derived
    from (subrun_id = job_id // num_fields, field_index = job_id %
    num_fields). All three are needed together to compute the chunk's
    task directories by pure arithmetic, with no directory listing
    involved.
    """

    job_id_start: int  # inclusive
    job_id_end: int  # inclusive
    num_fields: int


def _archive_filename(name: str, chunk: ChunkScope | None) -> str:
    """Names the archive for an output type.

    Args:
      name: the output type.
      chunk: the chunk the archive covers, or None for the whole run.

    Returns:
      <name>.tar, or <name>_<job id start>-<job id end>.tar for a chunk.
    """
    if chunk is None:
        return f"{name}.tar"
    return f"{name}_{chunk.job_id_start}-{chunk.job_id_end}.tar"


def _step_name(name: str, chunk: ChunkScope | None) -> str:
    if chunk is None:
        return f"merge-{name}"
    return f"merge-{name}-{chunk.job_id_start}-{chunk.job_id_end}"


def find_task_dirs(*, runs_dir: Path, run_name: str, chunk: ChunkScope | None = None) -> list[Path]:
    """Returns a run's stage-1 task directories.

    With chunk=None, the sorted directories on disk matching
    runs_dir/run_name/subrun_<id>/field_<index>. With a chunk, one directory
    per job id in its range, derived as (subrun_id, field_index) =
    divmod(job_id, num_fields) whether or not the directory exists.

    Args:
      runs_dir: base runs directory.
      run_name: run name.
      chunk: the job id range to restrict to, or None for every directory on disk.

    Returns:
      The task directories.
    """
    if chunk is None:
        return sorted((runs_dir / run_name).glob("subrun_*/field_*"))
    return [
        task_dir(
            runs_dir=runs_dir,
            run_name=run_name,
            subrun_id=job_id // chunk.num_fields,
            field_index=job_id % chunk.num_fields,
        )
        for job_id in range(chunk.job_id_start, chunk.job_id_end + 1)
    ]


def check_all_files_present(task_dirs: list[Path], filenames: Sequence[str]) -> None:
    """Raises RuntimeError if any task directory lacks any of filenames.

    Args:
      task_dirs: the task directories to check.
      filenames: the files each directory must hold.
    """
    missing: dict[Path, list[str]] = {}
    for d in task_dirs:
        missing_here = [filename for filename in filenames if not (d / filename).is_file()]
        if missing_here:
            missing[d] = missing_here
    if missing:
        details = "; ".join(f"{d}: missing {', '.join(files)}" for d, files in missing.items())
        raise RuntimeError(
            f"{len(missing)} of {len(task_dirs)} task dir(s) are missing expected stage-1 output -- "
            f"refusing to archive: {details}"
        )


def _write_tar(*, run_dir: Path, task_dirs: list[Path], filename: str, path: Path) -> None:
    """Writes a tar holding filename from each task directory.

    Args:
      run_dir: the directory each member's name is relative to.
      task_dirs: the directories to take filename from, archived in this order.
      filename: the file to archive from each directory.
      path: the tar to write.
    """
    with tarfile.open(path, "w") as tar:
        for d in task_dirs:
            src = d / filename
            member = tarfile.TarInfo(name=src.relative_to(run_dir).as_posix())
            member.size = src.stat().st_size
            member.mode = 0o644
            with src.open("rb") as f:
                tar.addfile(member, f)


def run_stage2_merge(
    *, runs_dir: Path, run_name: str, chunk: ChunkScope | None = None, types: Sequence[str] = ARCHIVE_TYPES
) -> None:
    """Archives stage-1 output into per-type tars under runs_dir/run_name/.

    Each type is a checkpointed step, so a rerun skips the types already
    archived. Raises ValueError for an unknown type, and RuntimeError if no
    task directory exists or any lacks a requested type's file.

    Args:
      runs_dir: base runs directory.
      run_name: run name.
      chunk: archives only this chunk's task directories into
        <type>_<job id start>-<job id end>.tar; None archives every task
        directory into <type>.tar.
      types: the output types to archive.
    """
    unknown = sorted(set(types) - set(ARCHIVE_TYPES))
    if unknown:
        raise ValueError(f"unknown type(s) {unknown}; expected a subset of {list(ARCHIVE_TYPES)}")
    run_dir = runs_dir / run_name
    steps = [(_step_name(name, chunk), name, OUTPUT_BY_NAME[name].filename) for name in ARCHIVE_TYPES if name in types]
    if all(is_checkpointed(run_dir, step) for step, _, _ in steps):
        logger.info("stage2 merge: every requested output already archived under %s", run_dir)
        return

    task_dirs = find_task_dirs(runs_dir=runs_dir, run_name=run_name, chunk=chunk)
    if not task_dirs:
        raise RuntimeError(f"No stage-1 task directories found under {run_dir}/subrun_*/field_*")
    logger.info("stage2 merge: found %d task dir(s) under %s", len(task_dirs), run_dir)
    check_all_files_present(task_dirs, [filename for _, _, filename in steps])

    for step, name, filename in steps:

        def archive(name=name, filename=filename) -> None:
            path = run_dir / _archive_filename(name, chunk)
            _write_tar(run_dir=run_dir, task_dirs=task_dirs, filename=filename, path=path)
            logger.info("wrote %s: %d task(s) archived", path, len(task_dirs))

        run_checkpointed(task_dir=run_dir, step_name=step, fn=archive)
