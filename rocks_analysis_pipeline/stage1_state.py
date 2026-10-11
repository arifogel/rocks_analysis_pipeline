"""Stage 1 combines three per-task steps -- running specsims, running
Katydid, then converting both tools' output to proto+zstd -- into a single
unit of work meant to run entirely on one wulf node: specsims's
bands.csv/dmtracks.csv output each get a proto+zstd conversion,
independent of Katydid, and Katydid's .root/slew-time output each get a
proto+zstd conversion in turn. The .speck, .root, and slew-time files
never need to leave that node's local scratch.

Only ever one acquisition per subrun (acquisition fixed at 0, decided
explicitly rather than left implicit -- see the proto schema, where it's
omitted entirely rather than stored as an always-0 field).

Naming note: task_dir is keyed on task identity alone (run_name, subrun_id,
field_index) -- the field value is data that belongs inside the task's
config/output, not something a caller needs to already know just to compute
where the task's directory lives.

State model: one flat, strictly ordered list of named steps (STEPS below),
each with an on-disk checkpoint marker -- not a hand-maintained state enum.
Every step, whether it does real compute (specsims, Katydid, the proto
conversion) or just cleanup (compressing/deleting the log, deleting a
previous step's output), happens in exactly one fixed sequence, each
strictly after the one before it. A flat list gives every property that
matters for free: idempotence (a step's marker existing means skip it),
resumability (run the list in order; already-done steps are no-ops), and a
"how far did this task get" query (the name of the furthest marker that
exists) -- with no separate categorization of "progress" vs. "cleanup"
steps needed, since a linear sequence has no combinatorial state space to
worry about regardless of what each step is named or does.

Deletion steps are separate entries in the list, distinct from the step
that justifies them (e.g. "delete .speck files" is a separate step after
"katydid_done", not folded into it): a task killed after deleting output
but before its marker gets written must not look identical to one that
never deleted anything. Retrying an already-done deletion is fine
(check-then-skip, or just tolerate ENOENT); silently having a compute
step's output deleted out from under it, with no record that this was
expected, is not.
"""

from pathlib import Path
from typing import Callable

from rocks_analysis_pipeline.checkpoints import is_checkpointed, run_checkpointed

# The one flat, ordered sequence of steps for a stage-1 task. Order here is
# the order they run in; there is no other structure.
STEP_SPECSIMS_DONE = "specsims_done"
STEP_SPECSIMS_LOG_COMPRESSED = "specsims_log_compressed"
STEP_UNCOMPRESSED_SPECSIMS_LOG_DELETED = "uncompressed_specsims_log_deleted"
# bands.csv, dmtracks.csv, tracks.csv (from Katydid's .root), and
# slew-times each get a proto conversion step: each is a separate,
# independently useful artifact, so bundling their conversion together
# would just mean unbundling it again downstream for no benefit.
# Deletion stays grouped per source (mc_truth_deleted covers both
# bands.csv and dmtracks.csv; katydid_output_deleted covers both .root
# and slew-times) rather than also going fully per-artifact: deletion is
# a cheap, already-idempotent unlink() either way, so it doesn't need the
# same per-artifact crash-safety granularity that production steps do
# (where the thing at risk is real, potentially expensive compute, not a
# trivial retry).
STEP_BANDS_PROTO_DONE = "bands_proto_done"
STEP_DMTRACKS_PROTO_DONE = "dmtracks_proto_done"
STEP_MC_TRUTH_DELETED = "mc_truth_deleted"  # bands.csv + dmtracks.csv, once converted
STEP_KATYDID_DONE = "katydid_done"
# Katydid's log gets the same compress-then-delete-uncompressed
# treatment as specsims's log, entirely independent of it: gated on
# katydid_done rather than specsims_done, since katydid.log doesn't exist
# until Katydid has run.
STEP_KATYDID_LOG_COMPRESSED = "katydid_log_compressed"
STEP_UNCOMPRESSED_KATYDID_LOG_DELETED = "uncompressed_katydid_log_deleted"
STEP_SPECSIMS_OUTPUT_DELETED = "specsims_output_deleted"  # .speck files, once Katydid has consumed them
STEP_EVENTS_PROTO_DONE = "events_proto_done"  # .root (MB-events tree) -> proto
STEP_POINTS_PROTO_DONE = "points_proto_done"  # .root (tracks tree) -> proto
STEP_SLEW_PROTO_DONE = "slew_proto_done"  # slew-times -> proto
STEP_KATYDID_OUTPUT_DELETED = "katydid_output_deleted"  # .root + slew-times, once converted

STEPS: list[str] = [
    STEP_SPECSIMS_DONE,
    STEP_SPECSIMS_LOG_COMPRESSED,
    STEP_UNCOMPRESSED_SPECSIMS_LOG_DELETED,
    STEP_BANDS_PROTO_DONE,
    STEP_DMTRACKS_PROTO_DONE,
    STEP_MC_TRUTH_DELETED,
    STEP_KATYDID_DONE,
    STEP_KATYDID_LOG_COMPRESSED,
    STEP_UNCOMPRESSED_KATYDID_LOG_DELETED,
    STEP_SPECSIMS_OUTPUT_DELETED,
    STEP_EVENTS_PROTO_DONE,
    STEP_POINTS_PROTO_DONE,
    STEP_SLEW_PROTO_DONE,
    STEP_KATYDID_OUTPUT_DELETED,
]


def task_dir(*, runs_dir: Path, run_name: str, subrun_id: int, field_index: int) -> Path:
    """The one directory holding everything for this stage-1 task: its
    config, its (possibly node-local-only) intermediate .speck/.root/slew
    files, its checkpoint markers, and its final proto+zstd output.
    """
    return runs_dir / run_name / f"subrun_{subrun_id}" / f"field_{field_index}"


def parse_task_dir(d: Path) -> tuple[str, int, int]:
    """The inverse of task_dir: recovers (run_name, subrun_id, field_index)
    from a task's directory path. A step function receives only task_dir,
    so this is the only way to recover these three values, e.g. to build a
    TaskIdentity. Safe to parse back out this way specifically because
    task_dir's path structure
    (runs_dir/run_name/subrun_{id}/field_{index}) is a stable convention
    this same module defines and controls, unlike parsing a physical
    quantity such as a field value out of a display string: the field
    value itself lives inside the task's config/output, not in task_dir's
    path.
    """
    field_index = int(d.name.removeprefix("field_"))
    subrun_id = int(d.parent.name.removeprefix("subrun_"))
    run_name = d.parent.parent.name
    return run_name, subrun_id, field_index


def get_state(*, runs_dir: Path, run_name: str, subrun_id: int, field_index: int) -> str | None:
    """Returns the name of the furthest-completed step for this task (per
    STEPS's order), or None if nothing has been checkpointed yet.
    Derived purely from which marker files exist on disk -- not tracked
    separately -- so a resumed/retried task (or an external monitoring
    query) can always determine exactly where a task stands just by looking
    at what's already there.
    """
    d = task_dir(runs_dir=runs_dir, run_name=run_name, subrun_id=subrun_id, field_index=field_index)
    furthest: str | None = None
    for step in STEPS:
        if is_checkpointed(d, step):
            furthest = step
        else:
            break
    return furthest


def run_stage1_task(
    *,
    runs_dir: Path,
    run_name: str,
    subrun_id: int,
    field_index: int,
    step_fns: dict[str, Callable[[Path], None]],
    skip_steps: frozenset[str] = frozenset(),
    stop_after: str | None = None,
) -> None:
    """Runs the steps in STEPS order for one task, resuming after whatever is already checkpointed.

    Each step function must leave safe output if rerun from a partial attempt:
    run_checkpointed guards against re-running a finished step, not against a
    step partially completing.

    Args:
      runs_dir: base runs directory.
      run_name: run name.
      subrun_id: the task's subrun.
      field_index: the task's field.
      step_fns: maps each entry in STEPS to the function that performs it, given the task's task_dir.
      skip_steps: steps to skip. Neither the step function nor its marker runs, so
        a skipped step is skipped again on every call.
      stop_after: the step after which to stop, or None to run through the last step. A step that
        is skipped or already checkpointed still counts as reached.
    """
    if stop_after is not None and stop_after not in STEPS:
        raise ValueError(f"unknown step {stop_after!r}; expected one of {STEPS}")
    d = task_dir(runs_dir=runs_dir, run_name=run_name, subrun_id=subrun_id, field_index=field_index)
    d.mkdir(parents=True, exist_ok=True)
    stop = False
    for step in STEPS:
        if stop:
            break
        stop = step == stop_after
        if step in skip_steps:
            continue
        run_checkpointed(task_dir=d, step_name=step, fn=lambda step=step: step_fns[step](d))
