"""Concrete implementations of stage1_state.STEPS, one function per step,
matching the Callable[[Path], None] shape run_stage1_task's own step_fns
expects (each takes the task's own task_dir as its only argument).

Kept separate from stage1_state.py on purpose: that module owns sequencing
(what order things happen in, how to resume), this one owns what each step
actually does. Every step is implemented for real. specsims_done and
katydid_done are the two exceptions to the plain Callable[[Path], None]
shape: make_run_specsims/make_run_katydid are factories, not step functions
directly, since those two steps need more than task_dir alone (the base
yaml/json configs, the katydid config, noise paths) -- see their own doc
comments for why. STEP_FNS below leaves those two as NotImplementedError
placeholders; a real caller (stage1_task.py) builds the two real closures
via those factories and overrides STEP_FNS's own placeholders with them.

This module is also the single source of truth for stage 1's on-disk file
layout (SPECSIMS_CONFIG_FILENAME, ROOT_FILENAME, etc. below).
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Callable

import compression.zstd as zstd
import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc
import numpy as np
import pandas as pd
import uproot
import yaml
from python.runfiles import runfiles

from api.v1 import band_pb2, dmtrack_pb2, slew_times_pb2, task_identity_pb2, track_pb2
from stage1_state import parse_task_dir

SPECSIMS_RLOCATION = "ghcss+/cmd/specsims/specsims_/specsims"  # matches local_spec_sims.py's own constant
KATYDID_RLOCATION = "katydid+/Source/Executables/Main/Katydid"  # matches local_ssa_katydid.py's own constant

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


def resolve_specsims_path() -> str:
    """Duplicated from local_spec_sims.py's own resolve_specsims_path,
    rather than importing it: local_spec_sims.py is a py_binary, not a
    py_library, and this project's own established pattern (see
    run_spec_sims_ghcss.py's own doc comment on why it exists rather than
    importing run_spec_sims.py) is to duplicate a small, self-contained
    piece rather than refactor an existing, working entry point into a
    library it was never designed to be. Keep this in sync with
    local_spec_sims.SPECSIMS_RLOCATION if that ever changes.
    """
    r = runfiles.Create()
    specsims_path = r.Rlocation(SPECSIMS_RLOCATION)
    if specsims_path is None or not Path(specsims_path).is_file():
        raise RuntimeError(
            f"Could not resolve the ghcss specsims binary via runfiles at "
            f"'{SPECSIMS_RLOCATION}' (got: {specsims_path}). If the "
            f"canonical repo name for the ghcss module, or the go_binary's "
            f"own runfile path, has changed, update SPECSIMS_RLOCATION at "
            f"the top of this file."
        )
    return specsims_path


def resolve_katydid_path() -> str:
    """Duplicated from local_ssa_katydid.py's own resolve_katydid_path, for
    the same reason as resolve_specsims_path above."""
    r = runfiles.Create()
    katydid_path = r.Rlocation(KATYDID_RLOCATION)
    if katydid_path is None or not Path(katydid_path).is_file():
        raise RuntimeError(
            f"Could not resolve the katydid binary via runfiles at "
            f"'{KATYDID_RLOCATION}' (got: {katydid_path}). If the "
            f"canonical repo name for the katydid module has changed, "
            f"update KATYDID_RLOCATION at the top of this file."
        )
    return katydid_path


def get_slope(true_field: float, frequency: float = 19.15e9) -> float:
    """Duplicated from local_ssa_katydid.py's own get_slope (itself reused
    verbatim from run_ssa_katydid.py), for the same reason as
    resolve_specsims_path above."""
    approx_power = sc.power_larmor(true_field, frequency)
    approx_energy = sc.freq_to_energy(frequency, true_field)
    return sc.df_dt(approx_energy, true_field, approx_power)


# The only Katydid processor type, anywhere in its own source tree, whose
# Configure() reads a "set-field" key -- see local_ssa_katydid.py's own
# KATYDID_SET_FIELD_PROCESSOR_TYPES for the full reasoning (identical here,
# duplicated rather than imported for the same reason as
# resolve_specsims_path above).
KATYDID_SET_FIELD_PROCESSOR_TYPES = frozenset({"multi-band-event-builder"})


def render_katydid_config(base_config_path: str, true_field: float, output_path: Path) -> None:
    """Duplicated from local_ssa_katydid.py's own render_katydid_config
    (see that function's own doc comment for the full reasoning on why this
    exists and how it targets processor instances by declared type), for
    the same reason as resolve_specsims_path above."""
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


def render_specsims_config(task_dir: Path, yaml_config: str, json_config: str, initial_seed: int) -> Path:
    """Renders this one task's own specsims.yaml -- a single-field
    equivalent of RunSpecSimsGhcss._create_configs_for_experiment
    (run_spec_sims_ghcss.py), which renders every field of a subrun at
    once; adapted here for exactly one field, since a stage-1 task is
    exactly one (subrun, field) pair. seed = initial_seed + subrun_id,
    matching local_spec_sims.py's own seed formula (seeds = range(
    initial_seed, initial_seed + num_subruns); seed = seeds[subrun_id])
    exactly. Returns the path it wrote to.

    Split out from make_run_specsims's own closure so this half -- the
    part with real, checkable logic -- is directly testable without
    needing to import or run he6_cres_spec_sims/ghcss at all.
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

    config_path = task_dir / SPECSIMS_CONFIG_FILENAME
    with open(config_path, "w") as f:
        yaml.dump(yaml_dict, f, default_flow_style=False, sort_keys=False)
    return config_path


def make_run_specsims(
    yaml_config: str,
    json_config: str,
    initial_seed: int,
    use_ghcss: bool = False,
) -> Callable[[Path], None]:
    """Builds the run_specsims step function for one stage-1 run, capturing
    yaml_config/json_config/initial_seed via closure -- run_stage1_task's
    own step_fns interface only ever passes task_dir itself (see
    run_stage1_task's own doc comment), so anything a step needs beyond
    that has to be captured this way rather than threaded through that
    interface.

    Renders this task's own specsims.yaml (see render_specsims_config's
    own doc comment), then actually runs it. Default (use_ghcss=False):
    calls he6_cres_spec_sims.simulation.Simulation.run_full() directly --
    the real simulation code path, not just config generation, unlike
    RunSpecSimsGhcss, whose entire reason for existing was avoiding this
    exact import (he6_cres_spec_sims transitively imports numpy/scipy).
    That tradeoff doesn't apply here: this step's whole job is running a
    simulation, so importing what does that is unavoidable. use_ghcss=True
    instead resolves and invokes the specsims Go binary as a subprocess,
    matching local_spec_sims.py's own existing invocation -- kept as an
    option per instruction, but not the default: ghcss's own measured
    throughput gain (35 MB/min vs. 31 MB/min from he6-cres-spec-sims)
    didn't justify the code-review surface of adopting it project-wide.
    """

    def fn(task_dir: Path) -> None:
        config_path = render_specsims_config(task_dir, yaml_config, json_config, initial_seed)

        # Line-buffered (buffering=1), matching local_spec_sims.py's own
        # reasoning exactly: without it, writes to a redirected sys.stdout
        # are fully block-buffered, so a log file tailed while the job is
        # still running can appear to lag far behind (or show nothing at
        # all) even though the job is progressing normally.
        #
        # try/finally to restore sys.stdout/sys.stderr afterward: this
        # function runs inside whatever process called run_stage1_task,
        # which (unlike local_spec_sims.py's own ProcessPoolExecutor
        # workers, which exit after one job) may go on to do other things
        # in the same process afterward.
        log_path = task_dir / LOG_FILENAME
        real_stdout, real_stderr = sys.stdout, sys.stderr
        with open(log_path, "w", buffering=1) as log_file:
            sys.stdout = log_file
            sys.stderr = log_file
            try:
                if use_ghcss:
                    specsims_path = resolve_specsims_path()
                    subprocess.run(
                        [specsims_path, "--config", str(config_path)],
                        check=True,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                    )
                else:
                    import he6_cres_spec_sims.simulation as he6_simulation

                    he6_simulation.Simulation(str(config_path)).run_full()
            finally:
                sys.stdout, sys.stderr = real_stdout, real_stderr

    return fn


def delete_specsims_output(task_dir: Path) -> None:
    """Deletes the .speck files (and their containing spec_files/
    directory), no longer needed once Katydid has consumed them. Only ever
    runs after katydid_done in STEPS's own fixed order.
    """
    spec_files_dir = task_dir / SPECSIMS_OUTPUT_DIRNAME / "spec_files"
    shutil.rmtree(spec_files_dir, ignore_errors=True)


def build_katydid_command_for_task(task_dir: Path, katydid_path: str, katydid_config: str, noise_paths: list[str]) -> list[str]:
    """Builds the Katydid command line for this task: renders this task's
    own copy of katydid_config with set-field set to its true_field (see
    render_katydid_config's own doc comment for why this matters -- see
    this project's own commit history: the existing local_ssa_katydid.py
    pipeline never did this at all before), then builds the full command
    against the two .speck files this task's own specsims run produced --
    the single-task equivalent of local_ssa_katydid.build_katydid_command,
    adapted from operating on a pandas row representing one of many tasks
    in a batch to operating on this one task's own task_dir directly.

    Split out from make_run_katydid's own closure so this half -- the part
    with real, checkable logic -- is directly testable without needing a
    real katydid_path or to actually invoke it.
    """
    identity = _task_identity(task_dir)
    true_field = identity.true_field

    rendered_katydid_config_path = task_dir / "katydid_config.yaml"
    render_katydid_config(katydid_config, true_field, rendered_katydid_config_path)

    spec_files_dir = task_dir / SPECSIMS_OUTPUT_DIRNAME / "spec_files"
    speck_paths = sorted(spec_files_dir.glob("*.speck"))
    if len(speck_paths) != 2:
        raise RuntimeError(
            f"Expected exactly 2 .speck files (one per channel) in {spec_files_dir}, "
            f"found {len(speck_paths)}: {speck_paths}"
        )

    approx_slope = get_slope(true_field)
    # Keep the LTF acceptance area to 45 bins (90 Hz*s) but scale f vs t
    # with slope based on good reconstruction at 0.711T and 2.00T --
    # matches local_ssa_katydid.build_katydid_command's own formula
    # exactly.
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
    return command


def make_run_katydid(katydid_config: str, noise_paths: list[str]) -> Callable[[Path], None]:
    """Builds the run_katydid step function for one stage-1 run, capturing
    katydid_config/noise_paths via closure, for the same reason as
    make_run_specsims above.
    """

    def fn(task_dir: Path) -> None:
        katydid_path = resolve_katydid_path()
        command = build_katydid_command_for_task(task_dir, katydid_path, katydid_config, noise_paths)
        subprocess.run(command, check=True)

    return fn


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


def _not_implemented_without_factory(step_name: str, factory_name: str):
    def fn(task_dir: Path) -> None:
        raise NotImplementedError(
            f"stage1_steps.STEP_FNS['{step_name}'] is a placeholder -- this step needs more than "
            f"task_dir alone, so it's built via {factory_name}(...) and substituted in by the "
            f"caller (see stage1_task.py), not usable directly from this module-level dict."
        )

    return fn


# Matches stage1_state.STEPS's own order exactly -- see that module.
STEP_FNS = {
    "specsims_done": _not_implemented_without_factory("specsims_done", "make_run_specsims"),
    "log_compressed": compress_log,
    "uncompressed_log_deleted": delete_uncompressed_log,
    "bands_proto_done": run_bands_proto_conversion,
    "dmtracks_proto_done": run_dmtracks_proto_conversion,
    "mc_truth_deleted": delete_mc_truth,
    "katydid_done": _not_implemented_without_factory("katydid_done", "make_run_katydid"),
    "specsims_output_deleted": delete_specsims_output,
    "tracks_proto_done": run_tracks_proto_conversion,
    "slew_proto_done": run_slew_proto_conversion,
    "katydid_output_deleted": delete_katydid_output,
}
