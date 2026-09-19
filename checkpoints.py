"""General-purpose checkpoint markers: on-disk, idempotent, crash-recoverable
tracking of "has step X been done for this task yet".

State lives entirely on the filesystem (one empty marker file per completed
step), not in any separate database or in-memory tracker -- so a task that
gets killed and re-run later can always determine exactly where it left off
just by looking at what's already there. This is deliberately prototyped
here, in the local-execution pipeline, before adapting it for the wulf
cluster-orchestration scripts (a different codebase -- see local_spec_sims.py's
own docstring on why local and cluster execution aren't the same scripts):
the pattern is meant to be reused (or closely ported) once wulf is back up,
not something specific to local execution.
"""

from pathlib import Path


def checkpoint_path(task_dir: Path, step_name: str) -> Path:
    return task_dir / f".checkpoint-{step_name}"


def is_checkpointed(task_dir: Path, step_name: str) -> bool:
    return checkpoint_path(task_dir, step_name).is_file()


def mark_checkpoint(task_dir: Path, step_name: str) -> None:
    """Records step_name as done for this task.

    Atomic: writes to a temp file in the same directory (so the following
    rename is same-filesystem, hence atomic on POSIX) and renames it into
    place, rather than creating the marker file directly -- a process killed
    mid-write must never leave behind a marker that looks complete but isn't
    (an empty file created directly is unlikely to be interrupted
    mid-write in practice, but the rename pattern costs nothing extra and
    generalizes to markers that ever need to carry real content, so it's
    used uniformly here rather than only where it's strictly necessary
    today).
    """
    path = checkpoint_path(task_dir, step_name)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.touch()
    tmp_path.rename(path)


def run_checkpointed(task_dir: Path, step_name: str, fn) -> None:
    """Runs fn() and marks step_name checkpointed, unless step_name is
    already checkpointed for this task (in which case fn() is skipped
    entirely). This is what makes a task resumable: re-running it after a
    crash re-executes only the steps that hadn't been checkpointed yet.

    fn itself must be safe to skip once its own output already exists on
    disk (i.e. this function guards against re-running fn, not against fn
    partially completing and leaving partial output behind -- fn is
    responsible for either writing its output atomically, e.g. write-then-
    rename, or being safely re-runnable from scratch on its own).
    """
    if is_checkpointed(task_dir, step_name):
        return
    fn()
    mark_checkpoint(task_dir, step_name)
