"""Concrete implementations of stage1_state.STEPS, one function per step,
matching the Callable[[Path], None] shape run_stage1_task's own step_fns
expects (each takes the task's own task_dir as its only argument).

Kept separate from stage1_state.py on purpose: that module owns sequencing
(what order things happen in, how to resume), this one owns what each step
actually does. Only the log-handling steps are implemented for real here --
the rest depend on decisions not made yet (Katydid command-line wiring, the
proto schema) and are left as explicit NotImplementedError stubs rather than
faked, so this module stays honestly incomplete instead of silently wrong.
"""

import shutil
from pathlib import Path

import compression.zstd as zstd

LOG_FILENAME = "local_spec_sims.log"
COMPRESSED_LOG_FILENAME = LOG_FILENAME + ".zst"


def compress_log(task_dir: Path) -> None:
    """Compresses task_dir's log file to <LOG_FILENAME>.zst, leaving the
    original in place (deletion is a separate step -- see
    stage1_state.py's own reasoning on why). Real, measured payoff, not a
    guess: this project's own local_spec_sims.log compressed
    4,871,860 -> 100,590 bytes (48.4x) in one real run, log text being far
    more repetitive than the numeric spec/speck data.

    Uses the stdlib compression.zstd module (available with zero extra
    dependencies: this repo's own MODULE.bazel/pyproject.toml already pin
    Python 3.14+, which is what added it -- see PEP 784) rather than
    shelling out to a system zstd binary, whose presence can't be assumed
    across every node of an HPC cluster (AlmaLinux 9's own default/minimal
    install doesn't appear to include it based on the available evidence,
    though this wasn't confirmed against official documentation).
    """
    log_path = task_dir / LOG_FILENAME
    compressed_path = task_dir / COMPRESSED_LOG_FILENAME
    with open(log_path, "rb") as f_in, zstd.open(compressed_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def delete_uncompressed_log(task_dir: Path) -> None:
    """Deletes the uncompressed log. Only ever runs after compress_log in
    STEPS's own fixed order, so the compressed copy is guaranteed to exist
    already. missing_ok=True: deleting an already-deleted file (e.g. this
    step re-running after a crash between the delete and its own checkpoint
    being recorded) is expected to be harmless, not an error.
    """
    (task_dir / LOG_FILENAME).unlink(missing_ok=True)


def _not_implemented(step_name: str):
    def fn(task_dir: Path) -> None:
        raise NotImplementedError(
            f"stage1_steps.{step_name}: not implemented yet -- depends on decisions not made yet "
            f"(Katydid command-line wiring and/or the proto schema)."
        )

    return fn


run_specsims = _not_implemented("run_specsims")
run_bands_proto_conversion = _not_implemented("run_bands_proto_conversion")  # bands.csv -> proto
run_dmtracks_proto_conversion = _not_implemented("run_dmtracks_proto_conversion")  # dmtracks.csv -> proto
delete_mc_truth = _not_implemented("delete_mc_truth")  # bands.csv + dmtracks.csv
run_katydid = _not_implemented("run_katydid")
delete_specsims_output = _not_implemented("delete_specsims_output")  # .speck files
run_tracks_proto_conversion = _not_implemented("run_tracks_proto_conversion")  # .root -> proto
run_slew_proto_conversion = _not_implemented("run_slew_proto_conversion")  # slew-times -> proto
delete_katydid_output = _not_implemented("delete_katydid_output")  # .root + slew

# Matches stage1_state.STEPS's own order exactly -- see that module.
STEP_FNS = {
    "specsims_done": run_specsims,
    "log_compressed": compress_log,
    "uncompressed_log_deleted": delete_uncompressed_log,
    "bands_proto_done": run_bands_proto_conversion,
    "dmtracks_proto_done": run_dmtracks_proto_conversion,
    "mc_truth_deleted": delete_mc_truth,
    "katydid_done": run_katydid,
    "specsims_output_deleted": delete_specsims_output,
    "tracks_proto_done": run_tracks_proto_conversion,
    "slew_proto_done": run_slew_proto_conversion,
    "katydid_output_deleted": delete_katydid_output,
}
