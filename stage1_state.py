"""Stage 1 combines what are, as of this writing, three separate local steps
(local_spec_sims.py -> local_ssa_katydid.py -> local_ssa_post_processing.py's
per-file .root reading) into a single per-task unit of work meant to run
entirely on one wulf node: specsims's own bands.csv/dmtracks.csv output each
get their own proto+zstd conversion (independent of Katydid entirely --
Katydid never touches either file), and Katydid's own .root/slew-time output
each get their own proto+zstd conversion in turn. The .speck, .root, and
slew-time files never need to leave that node's local scratch.

Only ever one acquisition per subrun (acquisition fixed at 0, decided
explicitly rather than left implicit -- see the proto schema, where it's
omitted entirely rather than stored as an always-0 field).

Naming note: task_dir is a deliberate departure from local_spec_sims.py's own
output_dir convention ("{field_index}_field_{field_value}T/", which embeds
the actual field value in the path). Stage 1 is a new pipeline stage, not a
reuse of that layout, so its own task directory is keyed on task identity
alone (run_name, subrun_id, field_index) -- the field value is data that
belongs inside the task's config/output, not something a caller needs to
already know just to compute where the task's own directory lives.

State model: one flat, strictly ordered list of named steps (STEPS below),
each with its own on-disk checkpoint marker -- not a hand-maintained state
enum. Every step, whether it does real compute (specsims, Katydid, the
proto conversion) or just cleanup (compressing/deleting the log, deleting a
previous step's now-unneeded output), happens in exactly one fixed sequence,
each strictly after the one before it. A flat list gives every property that
matters for free: idempotence (a step's own marker existing means skip it),
resumability (run the list in order; already-done steps are no-ops), and a
"how far did this task get" query (the name of the furthest marker that
exists) -- with no separate categorization of "progress" vs. "cleanup"
steps needed, since a linear sequence has no combinatorial state space to
worry about regardless of what each step is named or does.

Deletion steps are still their own separate entries in the list, distinct
from the step that justifies them (e.g. "delete .speck files" is its own
step after "katydid_done", not folded into it) -- a task killed after
deleting output but before its own marker gets written must not look
identical to one that never deleted anything: retrying an already-done
deletion is fine (check-then-skip, or just tolerate ENOENT), but silently
having a compute step's own output deleted out from under it, with no
record that this was expected, is not.
"""

from pathlib import Path
from typing import Callable

from checkpoints import is_checkpointed, run_checkpointed

# The one flat, ordered sequence of steps for a stage-1 task. Order here is
# the order they run in; there is no other structure.
STEP_SPECSIMS_DONE = "specsims_done"
STEP_LOG_COMPRESSED = "log_compressed"
STEP_UNCOMPRESSED_LOG_DELETED = "uncompressed_log_deleted"
# bands.csv, dmtracks.csv, tracks.csv (from Katydid's .root), and
# slew-times each get their own proto conversion step -- each is a
# separate file today and each is (or, for slew-times, may in the future
# be) an independently useful artifact on its own, so bundling their
# conversion together would just mean unbundling it again downstream for
# no benefit. Deletion stays grouped per source (mc_truth_deleted covers
# both bands.csv and dmtracks.csv; katydid_output_deleted covers both
# .root and slew-times) rather than also going fully per-artifact:
# deletion is a cheap, already-idempotent unlink() either way, so it
# doesn't need the same per-artifact crash-safety granularity that
# production steps do (where the thing at risk is real, potentially
# expensive compute, not a trivial retry).
STEP_BANDS_PROTO_DONE = "bands_proto_done"
STEP_DMTRACKS_PROTO_DONE = "dmtracks_proto_done"
STEP_MC_TRUTH_DELETED = "mc_truth_deleted"  # bands.csv + dmtracks.csv, no longer needed once converted
STEP_KATYDID_DONE = "katydid_done"
STEP_SPECSIMS_OUTPUT_DELETED = "specsims_output_deleted"  # .speck files, no longer needed once Katydid has consumed them
STEP_TRACKS_PROTO_DONE = "tracks_proto_done"  # .root -> proto
STEP_SLEW_PROTO_DONE = "slew_proto_done"  # slew-times -> proto
STEP_KATYDID_OUTPUT_DELETED = "katydid_output_deleted"  # .root + slew-times, no longer needed once converted

STEPS: list[str] = [
    STEP_SPECSIMS_DONE,
    STEP_LOG_COMPRESSED,
    STEP_UNCOMPRESSED_LOG_DELETED,
    STEP_BANDS_PROTO_DONE,
    STEP_DMTRACKS_PROTO_DONE,
    STEP_MC_TRUTH_DELETED,
    STEP_KATYDID_DONE,
    STEP_SPECSIMS_OUTPUT_DELETED,
    STEP_TRACKS_PROTO_DONE,
    STEP_SLEW_PROTO_DONE,
    STEP_KATYDID_OUTPUT_DELETED,
]


def task_dir(runs_dir: Path, run_name: str, subrun_id: int, field_index: int) -> Path:
    """The one directory holding everything for this stage-1 task: its
    config, its (possibly node-local-only) intermediate .speck/.root/slew
    files, its checkpoint markers, and its final proto+zstd output.
    """
    return runs_dir / run_name / f"subrun_{subrun_id}" / f"field_{field_index}"


def parse_task_dir(d: Path) -> tuple[str, int, int]:
    """The inverse of task_dir: recovers (run_name, subrun_id, field_index)
    from a task's own directory path. Needed because run_stage1_task's own
    step_fns only ever receive task_dir (see its own doc comment) -- a step
    that needs to build a TaskIdentity (see api/v1/task_identity.proto) has
    no other way to recover these three values. Safe to parse back out
    this way specifically because task_dir's own path structure
    (runs_dir/run_name/subrun_{id}/field_{index}) is a stable convention
    this same module defines and controls -- unlike, e.g., parsing a
    physical quantity like a field *value* out of a display string, which
    this project has specifically moved away from elsewhere (see
    task_dir's own doc comment on why it no longer embeds the field value).
    """
    field_index = int(d.name.removeprefix("field_"))
    subrun_id = int(d.parent.name.removeprefix("subrun_"))
    run_name = d.parent.parent.name
    return run_name, subrun_id, field_index


def get_state(runs_dir: Path, run_name: str, subrun_id: int, field_index: int) -> str | None:
    """Returns the name of the furthest-completed step for this task (per
    STEPS's own order), or None if nothing has been checkpointed yet.
    Derived purely from which marker files exist on disk -- not tracked
    separately -- so a resumed/retried task (or an external monitoring
    query) can always determine exactly where a task stands just by looking
    at what's already there.
    """
    d = task_dir(runs_dir, run_name, subrun_id, field_index)
    furthest: str | None = None
    for step in STEPS:
        if is_checkpointed(d, step):
            furthest = step
        else:
            break
    return furthest


def run_stage1_task(
    runs_dir: Path,
    run_name: str,
    subrun_id: int,
    field_index: int,
    step_fns: dict[str, Callable[[Path], None]],
    skip_steps: frozenset[str] = frozenset(),
) -> None:
    """Runs every step in STEPS, in order, for this task -- resuming
    correctly from wherever a previous attempt left off, since
    run_checkpointed skips any step whose marker already exists. This is
    the actual driver: it's what makes each step (including cleanup steps)
    happen at its fixed point in the sequence, every time this is called,
    rather than a fact that's merely checkable but never acted on.

    Matches, precisely:
        step = INITIAL_STEP
        while step != DONE:
            if not checkpointed(step) and not skipped(step):
                do_step(step)
                create_checkpoint(step)
            step = next_step(step)

    step_fns maps each entry in STEPS to the function that performs it
    (taking this task's own task_dir as its only argument) -- injected
    rather than hardcoded here, since the real implementations (the actual
    specsims/Katydid subprocess calls, the actual proto+zstd conversion)
    depend on pieces not decided yet (Katydid command-line wiring, the
    proto schema). Each step function is expected to produce its own output
    safely/resumably if it happens to be re-run from a partial attempt (see
    checkpoints.run_checkpointed's own doc comment) -- run_checkpointed
    guards against re-running a step that's already fully done, not against
    a step partially completing.

    skip_steps names steps to treat as skipped(step) above: neither
    step_fns[step] nor its checkpoint marker gets written, so a skipped
    step is re-evaluated (and re-skipped) on every future call rather than
    being permanently recorded as done -- this is what backs the
    --keep-<x> CLI flags (see stage1_task.py): a kept step's own delete
    action, and only that action, never runs and never gets checkpointed,
    while every other step proceeds normally.
    """
    d = task_dir(runs_dir, run_name, subrun_id, field_index)
    d.mkdir(parents=True, exist_ok=True)
    for step in STEPS:
        if step in skip_steps:
            continue
        run_checkpointed(d, step, lambda step=step: step_fns[step](d))
