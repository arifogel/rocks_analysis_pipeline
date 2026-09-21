"""Concrete implementations of stage1_state.STEPS, one function per step,
matching the Callable[[Path], None] shape run_stage1_task's own step_fns
expects (each takes the task's own task_dir as its only argument).

Kept separate from stage1_state.py on purpose: that module owns sequencing
(what order things happen in, how to resume), this one owns what each step
actually does. run_specsims and run_katydid are still explicit
NotImplementedError stubs (Katydid command-line wiring for the new
single-task shape, and which specsims implementation to call, are both
still open) -- everything else is implemented for real.

This module is also the single source of truth for stage 1's on-disk file
layout (SPECSIMS_CONFIG_FILENAME, ROOT_FILENAME, etc. below) -- run_specsims/
run_katydid, whenever they're written, need to write to exactly these paths
for the proto-conversion steps below to find their input.
"""

import shutil
from pathlib import Path

import compression.zstd as zstd
import pandas as pd
import uproot
import yaml

from api.v1 import band_pb2, dmtrack_pb2, slew_times_pb2, task_identity_pb2, track_pb2
from stage1_state import parse_task_dir

LOG_FILENAME = "local_spec_sims.log"
COMPRESSED_LOG_FILENAME = LOG_FILENAME + ".zst"

# specsims's own config and output layout within task_dir. Named
# "specsims.yaml" (not e.g. "config.yaml") specifically so
# Simulation.run_full()'s own config_path.stem-derived output directory
# comes out as "specsims/", not the meaningless "config/" a differently-
# named config file would produce.
SPECSIMS_CONFIG_FILENAME = "specsims.yaml"
SPECSIMS_OUTPUT_DIRNAME = "specsims"
BANDS_CSV_FILENAME = "bands.csv"
DMTRACKS_CSV_FILENAME = "dmtracks.csv"

# Katydid's own output layout within task_dir.
ROOT_FILENAME = "track.root"
SLEW_TIMES_FILENAME = "slew_times.txt"

# This module's own proto+zstd output layout within task_dir.
BANDS_PROTO_FILENAME = "bands.pb.zst"
DMTRACKS_PROTO_FILENAME = "dmtracks.pb.zst"
TRACKS_PROTO_FILENAME = "tracks.pb.zst"
SLEW_TIMES_PROTO_FILENAME = "slew_times.pb.zst"

# Katydid's own branch name (see api/v1/track.proto's own doc comment: the
# TLongTrackData class, Source/IO/Conversions/KTROOTData.hh) for the tree
# holding one task's reconstructed tracks.
TRACKS_TREE_NAME = "MultiBandEvent/fTracks"


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


def _task_identity(task_dir: Path) -> task_identity_pb2.TaskIdentity:
    """Builds this task's TaskIdentity: run_name/subrun_id/field_index
    parsed back out of task_dir itself (see stage1_state.parse_task_dir's
    own doc comment for why that's safe here), and true_field read from
    specsims.yaml -- the only place it exists anywhere in this pipeline
    (see this project's own commit history on why).
    """
    run_name, subrun_id, field_index = parse_task_dir(task_dir)
    with open(task_dir / SPECSIMS_CONFIG_FILENAME) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    identity = task_identity_pb2.TaskIdentity()
    identity.run_name = run_name
    identity.subrun_id = subrun_id
    identity.field_index = field_index
    identity.true_field = config["EventBuilder"]["main_field"]
    return identity


def _write_proto_zst(message, path: Path) -> None:
    """Serializes message and writes it, zstd-compressed, to path -- the
    shared tail end of every conversion function below."""
    with zstd.open(path, "wb") as f:
        f.write(message.SerializeToString())


def run_bands_proto_conversion(task_dir: Path) -> None:
    """bands.csv -> BandList -> bands.pb.zst. Column names in the real CSV
    (verified against an actual uploaded file -- see band.proto's own doc
    comment) already match Band's own snake_case field names exactly, so no
    column-name mapping is needed, unlike run_tracks_proto_conversion.
    """
    csv_path = task_dir / SPECSIMS_OUTPUT_DIRNAME / BANDS_CSV_FILENAME
    df = pd.read_csv(csv_path, index_col=0)

    band_list = band_pb2.BandList()
    band_list.task.CopyFrom(_task_identity(task_dir))
    for row in df.itertuples(index=False):
        b = band_list.bands.add()
        b.acquisition = int(row.acquisition)
        b.start_time = row.start_time
        b.start_freq = row.start_freq
        b.end_time = row.end_time
        b.end_freq = row.end_freq
        b.power = row.power
        b.event = int(row.event)
        b.track = int(row.track)
        b.band = int(row.band)

    _write_proto_zst(band_list, task_dir / BANDS_PROTO_FILENAME)


def run_dmtracks_proto_conversion(task_dir: Path) -> None:
    """dmtracks.csv -> DMTrackList -> dmtracks.pb.zst. Column names in the
    real CSV already match DMTrack's own snake_case field names exactly."""
    csv_path = task_dir / SPECSIMS_OUTPUT_DIRNAME / DMTRACKS_CSV_FILENAME
    df = pd.read_csv(csv_path, index_col=0)

    dmtrack_list = dmtrack_pb2.DMTrackList()
    dmtrack_list.task.CopyFrom(_task_identity(task_dir))
    for row in df.itertuples(index=False):
        d = dmtrack_list.dmtracks.add()
        # Every DMTrack field is a plain double (see dmtrack.proto's own doc
        # comment on why, including the five count-like columns) and the
        # CSV's own column order already matches the proto's field
        # declaration order exactly, so this can iterate positionally
        # rather than needing 38 named assignments.
        for value, field in zip(row, dmtrack_pb2.DMTrack.DESCRIPTOR.fields):
            setattr(d, field.name, value)

    _write_proto_zst(dmtrack_list, task_dir / DMTRACKS_PROTO_FILENAME)


def delete_mc_truth(task_dir: Path) -> None:
    """Deletes bands.csv and dmtracks.csv. Only ever runs after both
    bands_proto_done and dmtracks_proto_done in STEPS's own fixed order.
    missing_ok=True for the same reason as delete_uncompressed_log."""
    (task_dir / SPECSIMS_OUTPUT_DIRNAME / BANDS_CSV_FILENAME).unlink(missing_ok=True)
    (task_dir / SPECSIMS_OUTPUT_DIRNAME / DMTRACKS_CSV_FILENAME).unlink(missing_ok=True)


def _not_implemented(step_name: str):
    def fn(task_dir: Path) -> None:
        raise NotImplementedError(
            f"stage1_steps.{step_name}: not implemented yet -- depends on decisions not made yet "
            f"(which specsims implementation to call, and/or Katydid command-line wiring for the "
            f"single-task shape)."
        )

    return fn


run_specsims = _not_implemented("run_specsims")
run_katydid = _not_implemented("run_katydid")


def delete_specsims_output(task_dir: Path) -> None:
    """Deletes the .speck files (and their containing spec_files/
    directory), no longer needed once Katydid has consumed them. Only ever
    runs after katydid_done in STEPS's own fixed order.
    """
    spec_files_dir = task_dir / SPECSIMS_OUTPUT_DIRNAME / "spec_files"
    shutil.rmtree(spec_files_dir, ignore_errors=True)


def run_tracks_proto_conversion(task_dir: Path) -> None:
    """track.root -> TrackList -> tracks.pb.zst, reading the .root file
    directly via uproot (not through an intermediate CSV, so field types
    match the real ROOT branch types -- see track.proto's own doc comment).

    Unlike bands.csv/dmtracks.csv, ROOT branch names are PascalCase
    (TrackId, BandNumber, ...) while Track's own proto field names are
    snake_case (track_id, band_number, ...), so this needs an explicit
    name mapping rather than a positional or same-name correspondence.
    UniqueID/Bits (ROOT's own TObject bookkeeping, not Katydid data -- see
    track.proto's own doc comment) are simply not in this mapping, so
    they're dropped by omission.
    """
    branch_to_field = {
        "TrackId": "track_id",
        "EventId": "event_id",
        "BandNumber": "band_number",
        "EventType": "event_type",
        "AxialFreq": "axial_freq",
        "NumPoints": "num_points",
        "StartFrequency": "start_frequency",
        "EndFrequency": "end_frequency",
        "FreqLength": "freq_length",
        "StartTimeInRunC": "start_time_in_run_c",
        "EndTimeInRunC": "end_time_in_run_c",
        "TimeLength": "time_length",
        "StartTimeInAcqC": "start_time_in_acq_c",
        "EndTimeInAcqC": "end_time_in_acq_c",
        "StartAcqID": "start_acq_id",
        "AcqFreqIntercept": "acq_freq_intercept",
        "BulkSlope": "bulk_slope",
        "MeanLocalSlope": "mean_local_slope",
        "MaxLocalSlope": "max_local_slope",
        "MinLocalSlope": "min_local_slope",
        "StdDevLocalSlope": "std_dev_local_slope",
        "TotalNsp": "total_nsp",
        "MeanNsp": "mean_nsp",
        "MaxNsp": "max_nsp",
        "MinNsp": "min_nsp",
        "StdDevNsp": "std_dev_nsp",
        "TotalPower": "total_power",
        "MeanPower": "mean_power",
        "MaxPower": "max_power",
        "MinPower": "min_power",
        "StdDevPower": "std_dev_power",
        "ManhattanLength": "manhattan_length",
        "Density": "density",
        "NSPPerUnitLength": "nsp_per_unit_length",
        "DensityEstSNR": "density_est_snr",
        "MLEPowerSNR": "mle_power_snr",
    }

    root_path = task_dir / ROOT_FILENAME
    with uproot.open(root_path) as f:
        arrays = f[TRACKS_TREE_NAME].arrays(list(branch_to_field), library="np")

    track_list = track_pb2.TrackList()
    track_list.task.CopyFrom(_task_identity(task_dir))
    num_rows = len(next(iter(arrays.values()))) if arrays else 0
    for i in range(num_rows):
        t = track_list.tracks.add()
        for branch, field in branch_to_field.items():
            setattr(t, field, arrays[branch][i].item())

    _write_proto_zst(track_list, task_dir / TRACKS_PROTO_FILENAME)


def run_slew_proto_conversion(task_dir: Path) -> None:
    """slew_times.txt -> SlewTimesList -> slew_times.pb.zst."""
    txt_path = task_dir / SLEW_TIMES_FILENAME
    df = pd.read_csv(txt_path)

    slew_list = slew_times_pb2.SlewTimesList()
    slew_list.task.CopyFrom(_task_identity(task_dir))
    for row in df.itertuples(index=False):
        s = slew_list.slew_times.add()
        s.time_on = row.Time_On
        s.time_off = row.Time_Off

    _write_proto_zst(slew_list, task_dir / SLEW_TIMES_PROTO_FILENAME)


def delete_katydid_output(task_dir: Path) -> None:
    """Deletes track.root and slew_times.txt. Only ever runs after both
    tracks_proto_done and slew_proto_done in STEPS's own fixed order.
    missing_ok=True for the same reason as delete_uncompressed_log."""
    (task_dir / ROOT_FILENAME).unlink(missing_ok=True)
    (task_dir / SLEW_TIMES_FILENAME).unlink(missing_ok=True)


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
