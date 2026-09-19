"""Stage 1 combines what are, as of this writing, three separate local steps
(local_spec_sims.py -> local_ssa_katydid.py -> local_ssa_post_processing.py's
per-file .root reading) into a single per-task unit of work meant to run
entirely on one wulf node: specsims -> Katydid -> proto+zstd conversion,
with the .speck and .root/slew-time files never needing to leave that node's
local scratch.

Only ever one acquisition per subrun (acquisition fixed at 0, decided
explicitly rather than left implicit -- see the proto schema, where it's
omitted entirely rather than stored as an always-0 field).

Naming note: this is a deliberate departure from local_spec_sims.py's own
output_dir convention ("{field_index}_field_{field_value}T/", which embeds
the actual field value in the path). Stage 1 is a new pipeline stage, not a
reuse of that layout, so its own task directory is keyed on task identity
alone (run_name, subrun_id, field_index) -- the field value is data that
belongs inside the task's config/output, not something a caller needs to
already know just to compute where the task's own directory lives.
"""

from enum import Enum, auto
from pathlib import Path

from checkpoints import is_checkpointed

# --- progress steps: the linear "what do I run next" state machine ---
STEP_SPECSIMS_DONE = "specsims_done"
STEP_KATYDID_DONE = "katydid_done"
STEP_PROTO_DONE = "proto_done"

# --- cleanup steps: independent, best-effort, each gated by its own
# justifying progress step above, but not part of "what do I run next" and
# not ordered relative to each other. Each deletion is its own separate
# checkpoint from the step that justifies it: a task killed after deleting
# output but before recording that the deletion happened must not be
# indistinguishable from one that never deleted anything, since retrying an
# already-done deletion is fine (check-then-skip, or just tolerate ENOENT)
# but retrying a compute step whose output was silently already deleted out
# from under it is not.
STEP_LOG_COMPRESSED = "log_compressed"
STEP_LOG_DELETED = "log_deleted"
STEP_SPECSIMS_OUTPUT_DELETED = "specsims_output_deleted"  # .speck files, justified by katydid_done
STEP_KATYDID_OUTPUT_DELETED = "katydid_output_deleted"  # .root + slew-times files, justified by proto_done


class Stage1State(Enum):
    NOT_STARTED = auto()  # nothing done yet
    SPECSIMS_DONE = auto()  # .speck files + dmtracks.csv + bands.csv + log exist
    KATYDID_DONE = auto()  # .root + slew-times files exist
    PROTO_DONE = auto()  # proto.zstd exists -- stage 1 complete for this task


def task_dir(runs_dir: Path, run_name: str, subrun_id: int, field_index: int) -> Path:
    """The one directory holding everything for this stage-1 task: its
    config, its (possibly node-local-only) intermediate .speck/.root/slew
    files, its checkpoint markers, and its final proto+zstd output.
    """
    return runs_dir / run_name / f"subrun_{subrun_id}" / f"field_{field_index}"


def get_state(runs_dir: Path, run_name: str, subrun_id: int, field_index: int) -> Stage1State:
    """Determines this task's current progress state by checking which
    checkpoint markers exist on disk -- derived from the filesystem, not
    tracked separately, so a resumed/retried task can always determine
    where to pick up just by looking at what's already there. Checked
    furthest-first: PROTO_DONE implies KATYDID_DONE and SPECSIMS_DONE also
    happened at some point, even if their own cleanup steps have since
    removed the output those particular checkpoints refer to.
    """
    d = task_dir(runs_dir, run_name, subrun_id, field_index)
    if is_checkpointed(d, STEP_PROTO_DONE):
        return Stage1State.PROTO_DONE
    if is_checkpointed(d, STEP_KATYDID_DONE):
        return Stage1State.KATYDID_DONE
    if is_checkpointed(d, STEP_SPECSIMS_DONE):
        return Stage1State.SPECSIMS_DONE
    return Stage1State.NOT_STARTED
