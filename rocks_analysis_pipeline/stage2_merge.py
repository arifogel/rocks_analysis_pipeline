"""Stage 2: merges stage-1 task output into per-run BandLists/DMTrackLists/
EventLists/PointLists files -- runs_dir/run_name/{bands,dmtracks,events,
points}.pb.zst, sibling to that run's own subrun_*/ directories.

run_stage2_merge's chunk=None default merges every task directory the run
has into one unsuffixed set of those four files. Passing a ChunkScope
instead merges only that chunk's own task directories, into the same four
filenames with -<chunk index> inserted before their extensions -- see
ChunkScope's and run_stage2_merge's own doc comments. wulf_ssa.py uses this
to bound each reduce job's own output and memory footprint to one map
array's worth of tasks; local_ssa.py's own usage never chunks.

Deliberately a merge, not a flatten: each task's own BandList/DMTrackList/
EventList/PointList (already carrying its own TaskIdentity -- see
task_identity.proto's own doc comment) is gathered as-is into the wrapping
BandLists/DMTrackLists/EventLists/PointLists (see each one's own doc
comment in band.proto/dmtrack.proto/event.proto/point.proto), not exploded
into one flat list of rows. No per-row identity is lost, and no schema
change to Band/DMTrack/Event/Point themselves was needed for this.

slew_times is deliberately not merged here: assumed identical across every
task of a run (same DAQ config, same simulated acquisition timing), and not
consumed by cresproc regardless -- see this project's own commit history.

Strict by default: if any task directory is missing any of bands/dmtracks/
events/points .pb.zst, run_stage2_merge refuses to write anything at all --
not just for the affected type, the whole merge doesn't start.
allow_missing opts into the alternative: skip whatever's missing (per
type, with a warning), merging whatever is actually there.

Called directly (in-process, not as a subprocess) from local_ssa.py's own
main() after every stage-1 task for a run completes, since there's no
crash-isolation need for a pure read-and-merge step the way there was for
stage1_task's own simulation/Katydid work. Also has its own thin CLI driver
(stage2_merge_task.py) for the wulf case: stage 1 orchestrated separately
from this driver (see local_ssa.py's own module doc comment on why),
needing stage 2 run as its own, separate step against whatever stage-1
output already exists on disk.
"""

import logging
from pathlib import Path
from typing import Callable, NamedTuple, TypeVar

import compression.zstd as zstd

from rocks_analysis_pipeline.api.v1 import band_pb2, dmtrack_pb2, event_pb2, point_pb2
from rocks_analysis_pipeline.stage1_state import parse_task_dir
from rocks_analysis_pipeline.stage1_steps import (
    BANDS_PROTO_FILENAME,
    DMTRACKS_PROTO_FILENAME,
    EVENTS_PROTO_FILENAME,
    POINTS_PROTO_FILENAME,
)

logger = logging.getLogger(__name__)

ListMessage = TypeVar("ListMessage")


class ChunkScope(NamedTuple):
    """One reduce chunk's slice of a run: its 0-based position among the
    run's chunks, and the (uniform, run-wide) max tasks per chunk -- the
    same two values wulf_ssa.py's own --chunk-size chunking uses, so a
    chunk's own identity never depends on anything else about the run
    (how many chunks it has, how many tasks the last one happens to get).
    """

    chunk_index: int
    chunk_size: int


def _chunked_filename(filename: str, chunk: ChunkScope) -> str:
    """filename with -<chunk index> inserted before every extension
    (bands.pb.zst -> bands-0.pb.zst), unpadded -- nothing here needs to
    know how many chunks the run has in total.
    """
    stem, _, ext = filename.partition(".")
    return f"{stem}-{chunk.chunk_index}.{ext}"


def _all_task_dirs(runs_dir: Path, run_name: str) -> list[Path]:
    """Every stage-1 task directory for this run, in the same order
    wulf_ssa.py's own flat job_id = subrun_id * num_fields + field_index
    enumeration produces -- sorted numerically by (subrun_id, field_index),
    not lexicographically by path string (subrun_10 would otherwise sort
    before subrun_2). Discovered by globbing rather than computed from an
    expected count, so this merges whatever stage 1 actually produced
    (matching local_ssa_post_processing.py's own glob-based discovery, not
    stage1's own explicit range(num_subruns)/range(num_fields) computation
    -- there's no equivalent here of json_config's own fields_T to compute
    an expected count from, and glob discovery is what lets this run
    standalone against partial or already-cleaned-up runs).
    """
    dirs = (runs_dir / run_name).glob("subrun_*/field_*")
    return sorted(dirs, key=lambda d: parse_task_dir(d)[1:])


def find_task_dirs(runs_dir: Path, run_name: str, chunk: ChunkScope | None = None) -> list[Path]:
    """Every stage-1 task directory for this run (chunk=None), or just one
    chunk's own slice of them (chunk=<a ChunkScope>): position
    [chunk_index*chunk_size : chunk_index*chunk_size+chunk_size] of
    _all_task_dirs's own order.

    Safe without knowing num_fields, or the run's true total task count,
    because run_stage1_task's own task_dir.mkdir happens unconditionally as
    its very first action, before any step runs -- so by the time a chunk's
    reduce job starts (--dependency=afterany on that chunk's whole map
    array), every task dispatched to that array has its directory on disk,
    whether or not the task itself succeeded. Slicing _all_task_dirs's
    actual, already-on-disk list this way lands on exactly the same set
    job_id arithmetic would have, without computing it -- and the last
    chunk's slice simply comes up short against the list's real length,
    rather than needing the true total task count as a separate input to
    avoid overshooting it.
    """
    all_dirs = _all_task_dirs(runs_dir, run_name)
    if chunk is None:
        return all_dirs
    start = chunk.chunk_index * chunk.chunk_size
    return all_dirs[start : start + chunk.chunk_size]


def _read_proto_zst(message_cls: Callable[[], ListMessage], path: Path) -> ListMessage:
    with zstd.open(path, "rb") as f:
        message = message_cls()
        message.ParseFromString(f.read())
    return message


def _write_proto_zst(message, path: Path) -> None:
    """Duplicated from stage1_steps.py's own _write_proto_zst (a private
    helper there, not meant for import across a module boundary it wasn't
    designed for) -- same reasoning as that module's own
    resolve_specsims_path duplication precedent: small, self-contained
    piece, not worth reaching across for."""
    with zstd.open(path, "wb") as f:
        f.write(message.SerializeToString())


# Every proto type stage 2 merges -- the single source of truth for both
# check_all_files_present's own strict-mode validation and
# run_stage2_merge's own per-type merge calls, so the two can never drift
# out of sync with each other.
_MERGED_FILENAMES = (BANDS_PROTO_FILENAME, DMTRACKS_PROTO_FILENAME, EVENTS_PROTO_FILENAME, POINTS_PROTO_FILENAME)


def check_all_files_present(task_dirs: list[Path]) -> None:
    """Raises if any task directory is missing any of bands/dmtracks/
    events/points .pb.zst. Called once, upfront, before any merging starts
    (not per-type, mid-merge) -- so with allow_missing=False (the
    default), a missing file is caught before any output is written at
    all, not just for the affected type.
    """
    missing: dict[Path, list[str]] = {}
    for d in task_dirs:
        missing_here = [filename for filename in _MERGED_FILENAMES if not (d / filename).is_file()]
        if missing_here:
            missing[d] = missing_here
    if missing:
        details = "; ".join(f"{d}: missing {', '.join(files)}" for d, files in missing.items())
        raise RuntimeError(
            f"{len(missing)} of {len(task_dirs)} task dir(s) are missing expected stage-1 output -- "
            f"refusing to merge (pass allow_missing=True / --allow-missing to merge anyway, skipping "
            f"whatever's missing): {details}"
        )


def _merge_one_type(
    task_dirs: list[Path],
    filename: str,
    list_message_cls: Callable[[], ListMessage],
    lists_message_cls: Callable[[], object],
    lists_field_name: str,
) -> object:
    """Generic merge for one proto type: reads filename out of every task
    directory that has it (missing files -- a task that hasn't reached
    this step yet, or was run with --keep-<x> pointed elsewhere -- are
    skipped, not an error, matching local_ssa_post_processing.py's own
    "skip missing .root files" precedent) and gathers each one, as-is,
    into lists_message_cls's own repeated lists_field_name.
    """
    merged = lists_message_cls()
    field = getattr(merged, lists_field_name)
    n_missing = 0
    for d in task_dirs:
        p = d / filename
        if not p.is_file():
            n_missing += 1
            continue
        field.append(_read_proto_zst(list_message_cls, p))
    if n_missing:
        logger.warning("%d of %d task dir(s) missing %s; skipped", n_missing, len(task_dirs), filename)
    return merged


def merge_bands(task_dirs: list[Path]) -> band_pb2.BandLists:
    return _merge_one_type(task_dirs, BANDS_PROTO_FILENAME, band_pb2.BandList, band_pb2.BandLists, "band_lists")


def merge_dmtracks(task_dirs: list[Path]) -> dmtrack_pb2.DMTrackLists:
    return _merge_one_type(
        task_dirs, DMTRACKS_PROTO_FILENAME, dmtrack_pb2.DMTrackList, dmtrack_pb2.DMTrackLists, "dmtrack_lists"
    )


def merge_events(task_dirs: list[Path]) -> event_pb2.EventLists:
    return _merge_one_type(task_dirs, EVENTS_PROTO_FILENAME, event_pb2.EventList, event_pb2.EventLists, "event_lists")


def merge_points(task_dirs: list[Path]) -> point_pb2.PointLists:
    return _merge_one_type(task_dirs, POINTS_PROTO_FILENAME, point_pb2.PointList, point_pb2.PointLists, "point_lists")


def run_stage2_merge(
    runs_dir: Path, run_name: str, allow_missing: bool = False, chunk: ChunkScope | None = None
) -> None:
    """The actual stage-2 step: merges bands/dmtracks/events/points and
    writes each to runs_dir/run_name/<same filename as the per-task one>,
    sibling to that run's own subrun_*/ directories (per direct instruction
    on the output path convention) -- or, with chunk given, merges only
    that chunk's own task directories into the same four filenames, each
    with -<chunk index> inserted before its extension (see find_task_dirs's
    and _chunked_filename's own doc comments).

    allow_missing=False (the default): refuses to write anything at all
    if any task directory is missing any of the four expected files --
    see check_all_files_present's own doc comment. allow_missing=True
    skips whatever's missing per type instead (with a warning), merging
    whatever is actually there -- _merge_one_type's own long-standing
    skip logic already does this; the only thing that changes here is
    whether check_all_files_present runs first to rule it out entirely.
    """
    task_dirs = find_task_dirs(runs_dir, run_name, chunk)
    if not task_dirs:
        raise RuntimeError(f"No stage-1 task directories found under {runs_dir / run_name}/subrun_*/field_*")
    logger.info("stage2 merge: found %d task dir(s) under %s", len(task_dirs), runs_dir / run_name)

    if not allow_missing:
        check_all_files_present(task_dirs)

    run_dir = runs_dir / run_name

    def out_path(filename: str) -> Path:
        return run_dir / (filename if chunk is None else _chunked_filename(filename, chunk))

    merged_bands = merge_bands(task_dirs)
    bands_path = out_path(BANDS_PROTO_FILENAME)
    _write_proto_zst(merged_bands, bands_path)
    logger.info("wrote %s: %d task(s) merged", bands_path, len(merged_bands.band_lists))

    merged_dmtracks = merge_dmtracks(task_dirs)
    dmtracks_path = out_path(DMTRACKS_PROTO_FILENAME)
    _write_proto_zst(merged_dmtracks, dmtracks_path)
    logger.info("wrote %s: %d task(s) merged", dmtracks_path, len(merged_dmtracks.dmtrack_lists))

    merged_events = merge_events(task_dirs)
    events_path = out_path(EVENTS_PROTO_FILENAME)
    _write_proto_zst(merged_events, events_path)
    logger.info("wrote %s: %d task(s) merged", events_path, len(merged_events.event_lists))

    merged_points = merge_points(task_dirs)
    points_path = out_path(POINTS_PROTO_FILENAME)
    _write_proto_zst(merged_points, points_path)
    logger.info("wrote %s: %d task(s) merged", points_path, len(merged_points.point_lists))
