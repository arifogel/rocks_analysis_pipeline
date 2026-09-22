"""Stage 2: merges every stage-1 task's own bands/dmtracks/events/points
proto output for one run into a single, per-run BandLists/DMTrackLists/
EventLists/PointLists file each -- runs_dir/run_name/{bands,dmtracks,events,
points}.pb.zst, sibling to that run's own subrun_*/ directories.

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
from typing import Callable, TypeVar

import compression.zstd as zstd

from rocks_analysis_pipeline.api.v1 import band_pb2, dmtrack_pb2, event_pb2, point_pb2
from rocks_analysis_pipeline.stage1_steps import (
    BANDS_PROTO_FILENAME,
    DMTRACKS_PROTO_FILENAME,
    EVENTS_PROTO_FILENAME,
    POINTS_PROTO_FILENAME,
)

logger = logging.getLogger(__name__)

ListMessage = TypeVar("ListMessage")


def find_task_dirs(runs_dir: Path, run_name: str) -> list[Path]:
    """Every stage-1 task directory that actually exists for this run --
    matches stage1_state.task_dir's own path convention
    (runs_dir/run_name/subrun_<id>/field_<index>), discovered by globbing
    rather than computed from an expected count, so this merges whatever
    stage 1 actually produced (matching local_ssa_post_processing.py's own
    glob-based discovery, not stage1's own explicit range(num_subruns)/
    range(num_fields) computation -- there's no equivalent here of
    json_config's own fields_T to compute an expected count from, and glob
    discovery is what lets this run standalone against partial or
    already-cleaned-up runs).
    """
    return sorted((runs_dir / run_name).glob("subrun_*/field_*"))


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


def run_stage2_merge(runs_dir: Path, run_name: str, allow_missing: bool = False) -> None:
    """The actual stage-2 step: merges bands/dmtracks/events/points for
    one run and writes each to runs_dir/run_name/<same filename as the
    per-task one>, sibling to that run's own subrun_*/ directories (per
    direct instruction on the output path convention).

    allow_missing=False (the default): refuses to write anything at all
    if any task directory is missing any of the four expected files --
    see check_all_files_present's own doc comment. allow_missing=True
    skips whatever's missing per type instead (with a warning), merging
    whatever is actually there -- _merge_one_type's own long-standing
    skip logic already does this; the only thing that changes here is
    whether check_all_files_present runs first to rule it out entirely.
    """
    task_dirs = find_task_dirs(runs_dir, run_name)
    if not task_dirs:
        raise RuntimeError(f"No stage-1 task directories found under {runs_dir / run_name}/subrun_*/field_*")
    logger.info("stage2 merge: found %d task dir(s) under %s", len(task_dirs), runs_dir / run_name)

    if not allow_missing:
        check_all_files_present(task_dirs)

    run_dir = runs_dir / run_name

    merged_bands = merge_bands(task_dirs)
    _write_proto_zst(merged_bands, run_dir / BANDS_PROTO_FILENAME)
    logger.info("wrote %s: %d task(s) merged", run_dir / BANDS_PROTO_FILENAME, len(merged_bands.band_lists))

    merged_dmtracks = merge_dmtracks(task_dirs)
    _write_proto_zst(merged_dmtracks, run_dir / DMTRACKS_PROTO_FILENAME)
    logger.info(
        "wrote %s: %d task(s) merged", run_dir / DMTRACKS_PROTO_FILENAME, len(merged_dmtracks.dmtrack_lists)
    )

    merged_events = merge_events(task_dirs)
    _write_proto_zst(merged_events, run_dir / EVENTS_PROTO_FILENAME)
    logger.info("wrote %s: %d task(s) merged", run_dir / EVENTS_PROTO_FILENAME, len(merged_events.event_lists))

    merged_points = merge_points(task_dirs)
    _write_proto_zst(merged_points, run_dir / POINTS_PROTO_FILENAME)
    logger.info("wrote %s: %d task(s) merged", run_dir / POINTS_PROTO_FILENAME, len(merged_points.point_lists))
