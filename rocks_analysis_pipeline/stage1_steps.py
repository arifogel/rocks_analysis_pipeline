"""Concrete implementations of the stage-1 steps, one function per step.
Each takes the task's own task_dir as its only argument and returns None.

specsims_done and katydid_done are the two exceptions: make_run_specsims and
make_run_katydid are factories that build the real step closures, since
those two steps need more than task_dir alone (the base yaml/json configs,
the katydid config, noise paths). STEP_FNS below leaves those two as
NotImplementedError placeholders for a caller to override with the real
closures.

Single source of truth for stage 1's on-disk file layout
(SPECSIMS_CONFIG_FILENAME, ROOT_FILENAME, etc. below).
"""

import json
import logging
import shutil
import subprocess
import warnings
from pathlib import Path
from typing import Callable

import compression.zstd as zstd
import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc
import numpy as np
import pandas as pd
import uproot
import yaml

from rocks_analysis_pipeline.api.v1 import band_pb2, dmtrack_pb2, event_pb2, point_pb2, slew_times_pb2, task_identity_pb2
from rocks_analysis_pipeline.logging_setup import base_fmt
from rocks_analysis_pipeline.runfiles_resolve import resolve_executable
from rocks_analysis_pipeline.stage1_state import parse_task_dir

logger = logging.getLogger(__name__)

SPECSIMS_RLOCATION = "ghcss+/cmd/specsims/specsims_/specsims"
KATYDID_RLOCATION = "katydid+/release/katydid.sh"

SPECSIMS_LOG_FILENAME = "specsims.log"
COMPRESSED_SPECSIMS_LOG_FILENAME = SPECSIMS_LOG_FILENAME + ".zst"

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
KATYDID_LOG_FILENAME = "katydid.log"
COMPRESSED_KATYDID_LOG_FILENAME = KATYDID_LOG_FILENAME + ".zst"

# This module's own proto+zstd output layout within task_dir.
BANDS_PROTO_FILENAME = "bands.pb.zst"
DMTRACKS_PROTO_FILENAME = "dmtracks.pb.zst"
EVENTS_PROTO_FILENAME = "events.pb.zst"
POINTS_PROTO_FILENAME = "points.pb.zst"
SLEW_TIMES_PROTO_FILENAME = "slew_times.pb.zst"

# Katydid's top-level tree name (see KTROOTTreeTypeWriterEventAnalysis.cc's
# WriteMultiBandEvent). "MultiBandEvent" and "fTracks" are nested sub-branches *within* this
# tree, reached via chained indexing, not a slash-joined top-level tree path.
MB_EVENTS_TREE_NAME = "MB-events"

# Katydid's standalone long-track-finder output tree, separate from
# MB_EVENTS_TREE_NAME above: per-point data lives here, per-event data does
# not.
KATYDID_TRACKS_TREE_NAME = "tracks"


def _compress_log(*, task_dir: Path, log_filename: str, compressed_filename: str) -> None:
    """Compresses task_dir/log_filename to task_dir/compressed_filename,
    leaving the original in place.

    Uses the stdlib compression.zstd module (Python 3.14+) rather than
    shelling out to a system zstd binary, whose presence can't be assumed
    across every node of an HPC cluster.

    Shared by compress_specsims_log and compress_katydid_log below: the
    logic is identical for both, only the filenames differ. The two logs
    compress on independent schedules, gated on specsims_done and
    katydid_done respectively.
    """
    log_path = task_dir / log_filename
    compressed_path = task_dir / compressed_filename
    with open(log_path, "rb") as f_in, zstd.open(compressed_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    logger.info(
        "%s compressed: %d -> %d bytes (%.1fx)",
        log_filename,
        log_path.stat().st_size,
        compressed_path.stat().st_size,
        log_path.stat().st_size / max(compressed_path.stat().st_size, 1),
    )


def _delete_uncompressed_log(task_dir: Path, log_filename: str) -> None:
    """Deletes the uncompressed log. Runs after the matching compress step,
    so the compressed copy already exists. missing_ok=True tolerates
    deleting an already-deleted file, e.g. if this step reran after a
    crash between the delete and its checkpoint being recorded.
    """
    (task_dir / log_filename).unlink(missing_ok=True)
    logger.info("%s (uncompressed) deleted", log_filename)


def compress_specsims_log(task_dir: Path) -> None:
    _compress_log(
        task_dir=task_dir, log_filename=SPECSIMS_LOG_FILENAME, compressed_filename=COMPRESSED_SPECSIMS_LOG_FILENAME
    )


def delete_uncompressed_specsims_log(task_dir: Path) -> None:
    _delete_uncompressed_log(task_dir, SPECSIMS_LOG_FILENAME)


def compress_katydid_log(task_dir: Path) -> None:
    _compress_log(
        task_dir=task_dir, log_filename=KATYDID_LOG_FILENAME, compressed_filename=COMPRESSED_KATYDID_LOG_FILENAME
    )


def delete_uncompressed_katydid_log(task_dir: Path) -> None:
    _delete_uncompressed_log(task_dir, KATYDID_LOG_FILENAME)


def _task_identity(task_dir: Path) -> task_identity_pb2.TaskIdentity:
    """Builds this task's TaskIdentity: run_name/subrun_id/field_index
    parsed back out of task_dir via parse_task_dir, and true_field read
    from specsims.yaml -- the only place it exists in this pipeline.
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
    """bands.csv -> BandList -> bands.pb.zst. Column names in the CSV already
    match Band's snake_case field names, so no column-name mapping is needed.
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
    logger.info("bands_proto_done: %d bands", len(band_list.bands))


def run_dmtracks_proto_conversion(task_dir: Path) -> None:
    """dmtracks.csv -> DMTrackList -> dmtracks.pb.zst. Column names in the
    CSV already match DMTrack's snake_case field names."""
    csv_path = task_dir / SPECSIMS_OUTPUT_DIRNAME / DMTRACKS_CSV_FILENAME
    df = pd.read_csv(csv_path, index_col=0)

    dmtrack_list = dmtrack_pb2.DMTrackList()
    dmtrack_list.task.CopyFrom(_task_identity(task_dir))
    for row in df.itertuples(index=False):
        d = dmtrack_list.dmtracks.add()
        # Every DMTrack field is a double and the CSV's column order already
        # matches the proto's field declaration order, so this can iterate
        # positionally instead of writing 38 named assignments.
        for value, field in zip(row, dmtrack_pb2.DMTrack.DESCRIPTOR.fields):
            setattr(d, field.name, value)

    _write_proto_zst(dmtrack_list, task_dir / DMTRACKS_PROTO_FILENAME)
    logger.info("dmtracks_proto_done: %d dmtracks", len(dmtrack_list.dmtracks))


def delete_mc_truth(task_dir: Path) -> None:
    """Deletes bands.csv and dmtracks.csv. Runs after both bands_proto_done
    and dmtracks_proto_done complete. missing_ok=True tolerates an
    already-deleted file."""
    (task_dir / SPECSIMS_OUTPUT_DIRNAME / BANDS_CSV_FILENAME).unlink(missing_ok=True)
    (task_dir / SPECSIMS_OUTPUT_DIRNAME / DMTRACKS_CSV_FILENAME).unlink(missing_ok=True)
    logger.info("mc_truth_deleted: bands.csv, dmtracks.csv")


def resolve_specsims_path() -> str:
    """Resolves the ghcss specsims binary's real path via runfiles."""
    return resolve_executable(SPECSIMS_RLOCATION)


def resolve_katydid_path() -> str:
    """Resolves Katydid's real path via runfiles."""
    return resolve_executable(KATYDID_RLOCATION)


def get_slope(true_field: float, frequency: float = 19.15e9) -> float:
    approx_power = sc.power_larmor(true_field, frequency)
    approx_energy = sc.freq_to_energy(frequency, true_field)
    return sc.df_dt(approx_energy, true_field, approx_power)


# The only Katydid processor type whose Configure() reads a "set-field" key.
KATYDID_SET_FIELD_PROCESSOR_TYPES = frozenset({"multi-band-event-builder"})


def render_katydid_config(*, base_config_path: str, true_field: float, output_path: Path) -> None:
    """Writes a copy of base_config_path with every KATYDID_SET_FIELD_PROCESSOR_TYPES processor
    instance's set-field set to true_field."""
    with open(base_config_path) as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)

    processors = config_dict.get("processor-toolbox", {}).get("processors", [])
    target_names = [p["name"] for p in processors if p.get("type") in KATYDID_SET_FIELD_PROCESSOR_TYPES]
    if not target_names:
        raise ValueError(
            f"{base_config_path}: no processor instance of type in "
            f"{sorted(KATYDID_SET_FIELD_PROCESSOR_TYPES)} found in the processors: list -- "
            f"nowhere to write set-field."
        )
    for name in target_names:
        config_dict.setdefault(name, {})["set-field"] = float(true_field)

    with open(output_path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)


def render_specsims_config(
    *, task_dir: Path, yaml_config: str, json_config: str, initial_seed: int, noise_paths: list[str]
) -> Path:
    """Renders this one task's specsims.yaml for exactly one (subrun, field) pair, since a
    stage-1 task is exactly one such pair. seed = initial_seed + subrun_id. Returns the path it
    wrote to.

    noise_paths is written to yaml_dict["DAQ"]["noise_paths"] unconditionally: a resolved
    value is always available by the time this runs.
    """
    run_name, subrun_id, field_index = parse_task_dir(task_dir)
    seed = initial_seed + subrun_id

    with open(yaml_config) as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
    with open(json_config) as f:
        run_params = json.load(f)

    field = np.around(run_params["fields_T"][field_index], 6)
    trap = run_params["traps_A"][field_index]

    yaml_dict["Settings"]["rand_seed"] = int(seed)
    yaml_dict["Physics"]["events_to_simulate"] = int(run_params["events_to_simulate"])
    yaml_dict["Physics"]["betas_to_simulate"] = int(run_params["betas_to_simulate"])
    yaml_dict["EventBuilder"]["main_field"] = float(field)
    yaml_dict["EventBuilder"]["trap_current"] = float(trap)
    yaml_dict["DAQ"]["noise_paths"] = list(noise_paths)

    config_path = task_dir / SPECSIMS_CONFIG_FILENAME
    with open(config_path, "w") as f:
        yaml.dump(yaml_dict, f, default_flow_style=False, sort_keys=False)
    logger.info(
        "rendered %s: field=%s trap_current=%s seed=%s noise_paths=%s",
        config_path,
        field,
        trap,
        seed,
        noise_paths,
    )
    return config_path


def make_run_specsims(
    *,
    yaml_config: str,
    json_config: str,
    initial_seed: int,
    noise_paths: list[str],
    use_ghcss: bool = False,
) -> Callable[[Path], None]:
    """Builds the run_specsims step function for one stage-1 run, capturing
    yaml_config/json_config/initial_seed/noise_paths via closure, since a
    step function takes only task_dir.

    Renders this task's own specsims.yaml, then runs it. use_ghcss=False
    (the default) calls he6_cres_spec_sims.simulation.Simulation.run_full()
    directly. use_ghcss=True instead resolves and invokes the specsims Go
    binary as a subprocess: its measured throughput (35 MB/min vs.
    31 MB/min for he6-cres-spec-sims) doesn't justify the code-review
    surface of adopting it as the default.

    Log capture differs by branch. use_ghcss's subprocess has its stdout
    piped straight to the log file. The in-process branch instead attaches
    a logging.FileHandler to the "he6_cres_spec_sims" package logger
    (every module in it uses logging.getLogger(__name__), a descendant of
    that name) rather than root, so the handler doesn't also capture
    unrelated logging in this process; propagate=False keeps it from also
    reaching a root-level handler.

    This function sets no explicit level on the "he6_cres_spec_sims"
    logger, so it inherits its effective level from root regardless of
    propagate=False -- propagate only controls whether a record also
    reaches an ancestor's handler, not whether it's emitted. Root level is
    set once via logging_setup.init_logging at process startup; at the
    project's default of INFO, this keeps he6-cres-spec-sims's per-chunk
    debug lines (one line fires ~146,500 times per acquisition)
    suppressed.

    Also bridges the warnings module (numpy/scipy's own RuntimeWarning
    etc., which bypass logging entirely by default) into the same log
    file via logging.captureWarnings, scoped and restored the same way.
    """

    def fn(task_dir: Path) -> None:
        config_path = render_specsims_config(
            task_dir=task_dir,
            yaml_config=yaml_config,
            json_config=json_config,
            initial_seed=initial_seed,
            noise_paths=noise_paths,
        )
        log_path = task_dir / SPECSIMS_LOG_FILENAME
        logger.info("specsims starting (use_ghcss=%s), own log -> %s", use_ghcss, log_path)

        if use_ghcss:
            with open(log_path, "w", buffering=1) as log_file:
                specsims_path = resolve_specsims_path()
                subprocess.run(
                    [specsims_path, "--config", str(config_path)],
                    check=True,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                )
        else:
            he6_logger = logging.getLogger("he6_cres_spec_sims")
            handler = logging.FileHandler(log_path)
            handler.setFormatter(logging.Formatter(f"{base_fmt}: %(message)s"))
            he6_logger.addHandler(handler)
            prev_propagate = he6_logger.propagate
            he6_logger.propagate = False

            # Bridges Python's warnings module (numpy/scipy's RuntimeWarning
            # etc. bypass logging by default) into the same log file.
            # captureWarnings routes warnings.warn() through
            # logging.getLogger("py.warnings"), a different logger than
            # "he6_cres_spec_sims" above, so it needs the same handler
            # attached separately. Restored via warnings.showwarning
            # directly rather than captureWarnings(False), which would turn
            # capturing off unconditionally even if something else in this
            # process already enabled it. This logger's level is set
            # explicitly to DEBUG since captured warnings have no
            # meaningful INFO-vs-DEBUG distinction; the goal is only that
            # none get dropped by its own level filter, a narrower concern
            # than the general verbosity control at root.
            warnings_logger = logging.getLogger("py.warnings")
            warnings_logger.addHandler(handler)
            prev_warnings_level = warnings_logger.level
            prev_warnings_propagate = warnings_logger.propagate
            warnings_logger.setLevel(logging.DEBUG)
            warnings_logger.propagate = False
            prev_showwarning = warnings.showwarning
            logging.captureWarnings(True)

            try:
                import he6_cres_spec_sims.simulation as he6_simulation

                he6_simulation.Simulation(config_path).run_full()
            finally:
                warnings.showwarning = prev_showwarning
                warnings_logger.removeHandler(handler)
                warnings_logger.setLevel(prev_warnings_level)
                warnings_logger.propagate = prev_warnings_propagate

                he6_logger.removeHandler(handler)
                handler.close()
                he6_logger.propagate = prev_propagate

        logger.info("specsims complete")

    return fn


def delete_specsims_output(task_dir: Path) -> None:
    """Deletes the .speck files and their containing spec_files/ directory,
    once Katydid has consumed them. Runs after katydid_done.

    Also removes the specsims/ directory itself if it's now empty: by this
    point mc_truth_deleted has already removed bands.csv/dmtracks.csv, so
    this step's own spec_files/ removal is normally what leaves specsims/
    empty. rmdir only succeeds on a genuinely empty directory, so this is
    a no-op if specsims.yaml's own output produces anything else there.
    """
    specsims_dir = task_dir / SPECSIMS_OUTPUT_DIRNAME
    spec_files_dir = specsims_dir / "spec_files"
    shutil.rmtree(spec_files_dir, ignore_errors=True)
    dir_removed = False
    try:
        specsims_dir.rmdir()
        dir_removed = True
    except OSError:
        pass  # not empty (something else is in there) or already gone
    logger.info("specsims_output_deleted: spec_files/ (specsims/ dir also removed: %s)", dir_removed)


def build_katydid_command_for_task(
    *, task_dir: Path, katydid_path: str, katydid_config: str, noise_paths: list[str]
) -> list[str]:
    """Builds the Katydid command line for this task: renders a copy of katydid_config with
    set-field set to this task's true_field, then builds the full command against the two
    .speck files this task's specsims run produced.
    """
    identity = _task_identity(task_dir)
    true_field = identity.true_field

    rendered_katydid_config_path = task_dir / "katydid_config.yaml"
    render_katydid_config(
        base_config_path=katydid_config, true_field=true_field, output_path=rendered_katydid_config_path
    )

    spec_files_dir = task_dir / SPECSIMS_OUTPUT_DIRNAME / "spec_files"
    speck_paths = sorted(spec_files_dir.glob("*.speck"))
    if len(speck_paths) != 2:
        raise RuntimeError(
            f"Expected exactly 2 .speck files (one per channel) in {spec_files_dir}, "
            f"found {len(speck_paths)}: {speck_paths}"
        )

    approx_slope = get_slope(true_field)
    # Keep the LTF acceptance area to 45 bins (90 Hz*s) but scale f vs t
    # with slope based on good reconstruction at 0.711T and 2.00T.
    k = 0.1597 * approx_slope + 9.88e8
    if k <= 0:
        raise ValueError("No real positive solution (k must be > 0)")
    freq_accept = float(np.sqrt(90 * k))
    tgt = float(np.sqrt(90 / k))

    root_path = task_dir / ROOT_FILENAME
    slew_path = task_dir / SLEW_TIMES_FILENAME

    command = [katydid_path, "-c", str(rendered_katydid_config_path)]
    for i in range(2):
        command.append(f"--spec1.filenames_{i}={noise_paths[i]}")
    for i in range(2):
        command.append(f"--spec2.filenames_{i}={speck_paths[i]}")
    command.append(f"--long-tr-find.frequency-acceptance={freq_accept}")
    command.append(f"--long-tr-find.time-gap-tolerance={tgt}")
    command.append(f"--long-tr-find.initial-slope={approx_slope}")
    command.append(f"--long-tr-find.min-slope={approx_slope - 1e10}")
    command.append(f"--rtw.output-file={root_path}")
    command.append(f"--brw.output-file={root_path}")
    command.append(f"--stv.output-file={slew_path}")
    command.append("--log-level=PROG")
    logger.info("katydid command: %s", " ".join(command))
    return command


def make_run_katydid(katydid_config: str, noise_paths: list[str]) -> Callable[[Path], None]:
    """Builds the run_katydid step function for one stage-1 run, capturing
    katydid_config/noise_paths via closure, since a step function takes
    only task_dir.

    Katydid's own stdout/stderr (its C++ logging -- factory registrations,
    welcome banner, PROG/WARN lines, etc.) is piped to its own log file
    (KATYDID_LOG_FILENAME, a different step's output from
    SPECSIMS_LOG_FILENAME, each opened in "w" mode) rather than inherited
    from the parent process, which would otherwise send it straight to the
    console.
    """

    def fn(task_dir: Path) -> None:
        katydid_path = resolve_katydid_path()
        command = build_katydid_command_for_task(
            task_dir=task_dir, katydid_path=katydid_path, katydid_config=katydid_config, noise_paths=noise_paths
        )
        log_path = task_dir / KATYDID_LOG_FILENAME
        logger.info("katydid starting, own log -> %s", log_path)
        with open(log_path, "w", buffering=1) as log_file:
            subprocess.run(command, check=True, stdout=log_file, stderr=subprocess.STDOUT)
        logger.info("katydid complete")

    return fn


def run_events_proto_conversion(task_dir: Path) -> None:
    """track.root -> EventList -> events.pb.zst, reading the .root file
    directly via uproot.

    Reads from f["MB-events"]["MultiBandEvent"]["fTracks"]. "MultiBandEvent"
    and "fTracks" are nested sub-branches *within* the "MB-events" tree, not
    a slash-joined top-level tree path. The separate, standalone "tracks"
    tree (Katydid's WriteLongTrack output) holds pre-event-building
    long-track candidates, a dataset distinct from the post-event-building
    tracks kept in each MultiBandEvent.

    Each scalar per-track branch is named "fTracks.f<Name>" (e.g.
    "fTracks.fTrackId"); branch_to_field's keys are already the
    stripped form ("TrackId"). The nested, jagged fPoints sub-array
    (per-point data within each track) is skipped via its AsObjects
    interpretation.

    Flattening uses np.concatenate over each field's per-event jagged
    sub-arrays (bulk numpy, no pandas).

    ROOT branch names are PascalCase (TrackId, BandNumber, ...) while Event's proto field names
    are snake_case (track_id, band_number, ...), so this needs an explicit name mapping rather
    than a positional or same-name correspondence. UniqueID/Bits (ROOT's TObject bookkeeping,
    not Katydid data) aren't in this mapping, so they're dropped by omission -- along with
    fPoints, they're the only branches this leaves unread.
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
    branch_prefix = "fTracks.f"

    root_path = task_dir / ROOT_FILENAME
    with uproot.open(root_path) as f:
        tracks_root = f[MB_EVENTS_TREE_NAME]["MultiBandEvent"]["fTracks"]

        flat_fields: dict[str, np.ndarray] = {}
        for key, branch in tracks_root.items():
            if branch.interpretation.__class__.__name__ == "AsObjects":
                continue  # skips fPoints, the nested per-point sub-array
            name = key[len(branch_prefix):] if key.startswith(branch_prefix) else key
            field = branch_to_field.get(name)
            if field is None:
                continue
            nested = branch.array(library="np")
            flat_fields[field] = np.concatenate(nested) if len(nested) else np.array([])

    event_list = event_pb2.EventList()
    event_list.task.CopyFrom(_task_identity(task_dir))
    num_rows = len(next(iter(flat_fields.values()))) if flat_fields else 0
    for i in range(num_rows):
        e = event_list.events.add()
        for field, values in flat_fields.items():
            setattr(e, field, values[i].item())

    _write_proto_zst(event_list, task_dir / EVENTS_PROTO_FILENAME)
    logger.info("events_proto_done: %d events", len(event_list.events))


def run_points_proto_conversion(task_dir: Path) -> None:
    """track.root -> PointList -> points.pb.zst, reading the .root file
    directly via uproot.

    Reads from the standalone "tracks" tree (KATYDID_TRACKS_TREE_NAME) --
    Katydid's WriteLongTrack output, a tree separate from the
    post-event-building tracks kept in each event -- specifically the
    nested Track/fPoints/f* branches (Point's per-point member fields,
    TClonesArray-nested within each TLongTrackData entry). uproot's key
    format here uses a "/" for structural sub-branch nesting
    (Track/fPoints/...) and a "." for the innermost, dotted C++ member
    name (...fPoints.f<Name>).

    track_id is the real TrackId field, not a synthetic per-file index:
    this pipeline runs one acquisition per task, so TrackId is already
    unique within this output.

    Flattening uses np.concatenate over each field's per-event jagged
    sub-arrays (bulk numpy, no pandas).
    """
    branch_to_field = {
        "TrackId": "track_id",
        "EventId": "event_id",
        "BandNumber": "band_number",
        "Frequency": "frequency",
        "TimeInRunC": "time_in_run_c",
        "TimeInAcqC": "time_in_acq_c",
        "AcquisitionID": "acquisition_id",
        "Ordinate": "ordinate",
        "Threshold": "threshold",
        "NSP": "nsp",
        "NoiseMean": "noise_mean",
        "NoiseTau": "noise_tau",
        "NoiseVariance": "noise_variance",
        "TrackFinderLocalSlope": "track_finder_local_slope",
    }
    branch_prefix = "Track/fPoints/f"

    root_path = task_dir / ROOT_FILENAME
    with uproot.open(root_path) as f:
        tree = f[KATYDID_TRACKS_TREE_NAME]
        selected_columns = [c for c in tree.keys() if c.startswith(branch_prefix)]

        flat_fields: dict[str, np.ndarray] = {}
        if selected_columns:
            raw_dict = tree.arrays(selected_columns, library="np")
            for c in selected_columns:
                name = c.split(".")[-1][1:]
                field = branch_to_field.get(name)
                if field is None:
                    continue
                nested = raw_dict[c]
                flat_fields[field] = np.concatenate(nested) if len(nested) else np.array([])

    point_list = point_pb2.PointList()
    point_list.task.CopyFrom(_task_identity(task_dir))
    num_rows = len(next(iter(flat_fields.values()))) if flat_fields else 0
    for i in range(num_rows):
        p = point_list.points.add()
        for field, values in flat_fields.items():
            setattr(p, field, values[i].item())

    _write_proto_zst(point_list, task_dir / POINTS_PROTO_FILENAME)
    logger.info("points_proto_done: %d points", len(point_list.points))


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
    logger.info("slew_proto_done: %d slew_times rows", len(slew_list.slew_times))


def delete_katydid_output(task_dir: Path) -> None:
    """Deletes track.root and slew_times.txt. Runs after events_proto_done,
    points_proto_done, and slew_proto_done complete. missing_ok=True
    tolerates an already-deleted file."""
    (task_dir / ROOT_FILENAME).unlink(missing_ok=True)
    (task_dir / SLEW_TIMES_FILENAME).unlink(missing_ok=True)
    logger.info("katydid_output_deleted: track.root, slew_times.txt")


def _not_implemented_without_factory(step_name: str, factory_name: str):
    def fn(task_dir: Path) -> None:
        raise NotImplementedError(
            f"stage1_steps.STEP_FNS['{step_name}'] is a placeholder -- this step needs more than "
            f"task_dir alone, so it's built via {factory_name}(...) and substituted in by the "
            f"caller, not usable directly from this module-level dict."
        )

    return fn


# Keys are the stage-1 step names, in the order they run.
STEP_FNS = {
    "specsims_done": _not_implemented_without_factory("specsims_done", "make_run_specsims"),
    "specsims_log_compressed": compress_specsims_log,
    "uncompressed_specsims_log_deleted": delete_uncompressed_specsims_log,
    "bands_proto_done": run_bands_proto_conversion,
    "dmtracks_proto_done": run_dmtracks_proto_conversion,
    "mc_truth_deleted": delete_mc_truth,
    "katydid_done": _not_implemented_without_factory("katydid_done", "make_run_katydid"),
    "katydid_log_compressed": compress_katydid_log,
    "uncompressed_katydid_log_deleted": delete_uncompressed_katydid_log,
    "specsims_output_deleted": delete_specsims_output,
    "events_proto_done": run_events_proto_conversion,
    "points_proto_done": run_points_proto_conversion,
    "slew_proto_done": run_slew_proto_conversion,
    "katydid_output_deleted": delete_katydid_output,
}
