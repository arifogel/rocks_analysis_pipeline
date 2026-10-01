"""General-purpose checkpoint markers: on-disk, idempotent, crash-recoverable
tracking of "has step X been done for this task yet".

State lives entirely on the filesystem (one empty marker file per completed
step), not in any separate database or in-memory tracker, so a task that
gets killed and re-run later can always determine exactly where it left off
just by looking at what's already there. The pattern is general enough to
reuse or closely port for cluster orchestration, not something specific to
local execution.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def checkpoint_path(task_dir: Path, step_name: str) -> Path:
    return task_dir / f".checkpoint-{step_name}"


def is_checkpointed(task_dir: Path, step_name: str) -> bool:
    return checkpoint_path(task_dir, step_name).is_file()


def mark_checkpoint(task_dir: Path, step_name: str) -> None:
    """Records step_name as done for this task.

    Atomic: writes to a temp file in the same directory, then renames it
    into place, rather than creating the marker file directly. The rename
    is same-filesystem, hence atomic on POSIX, so a process killed
    mid-write never leaves behind a marker that looks complete but isn't.
    The pattern generalizes to markers that carry real content, so it's
    used uniformly here.
    """
    path = checkpoint_path(task_dir, step_name)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.touch()
    tmp_path.rename(path)


def run_checkpointed(*, task_dir: Path, step_name: str, fn) -> None:
    """Runs fn() and marks step_name checkpointed, unless step_name is
    already checkpointed for this task, in which case fn() is skipped
    entirely. This is what makes a task resumable: re-running it after a
    crash re-executes only the steps that hadn't been checkpointed yet.

    This function guards against re-running fn, not against fn partially
    completing and leaving partial output behind. fn is responsible for
    either writing its output atomically, e.g. write-then-rename, or
    being safely re-runnable from scratch.
    """
    if is_checkpointed(task_dir, step_name):
        logger.info("skipping %s: already checkpointed", step_name)
        return
    fn()
    mark_checkpoint(task_dir, step_name)
