"""Stage 2: archives stage-1 task output into per-run tar files --
runs_dir/run_name/{bands,dmtracks,events,points}.tar, sibling to that run's
subrun_*/ directories.

Each tar holds one stage-1 file per task, byte for byte, as
subrun_<id>/field_<index>/<filename>. Every member keeps the TaskIdentity
inside its own BandList/DMTrackList/EventList/PointList, so no per-task
identity is lost. A reader streams the tar one member at a time.

run_stage2_merge's chunk=None default archives every task directory the run
has into one unsuffixed set of those four files. Passing a ChunkScope
instead archives only that chunk's task directories, into filenames with
_<job id start>-<job id end> inserted before the extension. Memory use is
one copy buffer, whatever the chunk size.

slew_times is not archived here: identical across every task of a run (same
DAQ config, same simulated acquisition timing), and not consumed
downstream.

Strict: if any task directory is missing any of bands/dmtracks/events/points
.pb.zst, run_stage2_merge raises before writing anything.

Each of the four archives is a checkpointed step in runs_dir/run_name/
(.checkpoint-merge-<type>, with -<job id start>-<job id end> appended for a
chunk). Rerunning skips the steps already done and returns at once when all
four are.

Runs in-process, not as a subprocess, since a pure copy step needs no crash
isolation. Also has a thin CLI driver (stage2_merge_task.py) for running
stage 2 as a separate step against whatever stage-1 output already exists
on disk.
"""

import logging
import tarfile
from dataclasses import dataclass
from pathlib import Path

from rocks_analysis_pipeline.checkpoints import is_checkpointed, run_checkpointed
from rocks_analysis_pipeline.stage1_state import task_dir
from rocks_analysis_pipeline.stage1_steps import (
    BANDS_PROTO_FILENAME,
    DMTRACKS_PROTO_FILENAME,
    EVENTS_PROTO_FILENAME,
    POINTS_PROTO_FILENAME,
)

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


# (name, stage-1 filename) for each archived type. name keys the type's
# checkpoint and output filename.
_ARCHIVES = (
    ("bands", BANDS_PROTO_FILENAME),
    ("dmtracks", DMTRACKS_PROTO_FILENAME),
    ("events", EVENTS_PROTO_FILENAME),
    ("points", POINTS_PROTO_FILENAME),
)
_ARCHIVED_FILENAMES = tuple(filename for _, filename in _ARCHIVES)


def _archive_filename(name: str, chunk: ChunkScope | None) -> str:
    """The output filename for type name: <name>.tar, or
    <name>_<job id start>-<job id end>.tar for a chunk.
    """
    if chunk is None:
        return f"{name}.tar"
    return f"{name}_{chunk.job_id_start}-{chunk.job_id_end}.tar"


def _step_name(name: str, chunk: ChunkScope | None) -> str:
    if chunk is None:
        return f"merge-{name}"
    return f"merge-{name}-{chunk.job_id_start}-{chunk.job_id_end}"


def find_task_dirs(*, runs_dir: Path, run_name: str, chunk: ChunkScope | None = None) -> list[Path]:
    """Every stage-1 task directory for this run (chunk=None), or just the
    task directories for one chunk's job id range (chunk=<a ChunkScope>).

    chunk=None matches task_dir's path convention
    (runs_dir/run_name/subrun_<id>/field_<index>), discovered by globbing
    rather than computed from an expected count, so this archives whatever
    stage 1 actually produced and runs standalone against partial or
    already-cleaned-up runs.

    chunk=<a ChunkScope> instead computes the chunk's task dirs directly
    from its job id range (subrun_id, field_index = divmod(job_id,
    num_fields)): the expected set is already known exactly, so no glob is
    needed. A task dir that doesn't exist on disk is still returned:
    check_all_files_present treats every one of its (necessarily absent)
    files as missing, the same as a directory that exists but is
    incomplete.
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


def check_all_files_present(task_dirs: list[Path]) -> None:
    """Raises if any task directory is missing any of bands/dmtracks/
    events/points .pb.zst. Called once, upfront, before any archiving starts.
    """
    missing: dict[Path, list[str]] = {}
    for d in task_dirs:
        missing_here = [filename for filename in _ARCHIVED_FILENAMES if not (d / filename).is_file()]
        if missing_here:
            missing[d] = missing_here
    if missing:
        details = "; ".join(f"{d}: missing {', '.join(files)}" for d, files in missing.items())
        raise RuntimeError(
            f"{len(missing)} of {len(task_dirs)} task dir(s) are missing expected stage-1 output -- "
            f"refusing to merge: {details}"
        )


def _write_tar(*, run_dir: Path, task_dirs: list[Path], filename: str, path: Path) -> None:
    """Writes path as a tar of filename from every task directory, in the
    order given, each member named by its path relative to run_dir.
    """
    with tarfile.open(path, "w") as tar:
        for d in task_dirs:
            src = d / filename
            member = tarfile.TarInfo(name=src.relative_to(run_dir).as_posix())
            member.size = src.stat().st_size
            member.mode = 0o644
            with src.open("rb") as f:
                tar.addfile(member, f)


def run_stage2_merge(*, runs_dir: Path, run_name: str, chunk: ChunkScope | None = None) -> None:
    """The actual stage-2 step: archives bands/dmtracks/events/points into
    runs_dir/run_name/<type>.tar, sibling to that run's subrun_*/
    directories. With chunk given, archives only that chunk's task
    directories into <type>_<job id start>-<job id end>.tar.

    Each type is a checkpointed step in runs_dir/run_name/, so a rerun
    skips the types already archived. Raises if any task directory is
    missing any of the four expected files.
    """
    run_dir = runs_dir / run_name
    steps = [(_step_name(name, chunk), name, filename) for name, filename in _ARCHIVES]
    if all(is_checkpointed(run_dir, step) for step, _, _ in steps):
        logger.info("stage2 merge: every output already archived under %s", run_dir)
        return

    task_dirs = find_task_dirs(runs_dir=runs_dir, run_name=run_name, chunk=chunk)
    if not task_dirs:
        raise RuntimeError(f"No stage-1 task directories found under {run_dir}/subrun_*/field_*")
    logger.info("stage2 merge: found %d task dir(s) under %s", len(task_dirs), run_dir)
    check_all_files_present(task_dirs)

    for step, name, filename in steps:

        def archive(name=name, filename=filename) -> None:
            path = run_dir / _archive_filename(name, chunk)
            _write_tar(run_dir=run_dir, task_dirs=task_dirs, filename=filename, path=path)
            logger.info("wrote %s: %d task(s) archived", path, len(task_dirs))

        run_checkpointed(task_dir=run_dir, step_name=step, fn=archive)
