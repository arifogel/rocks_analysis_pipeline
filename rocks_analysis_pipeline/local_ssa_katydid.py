#!/usr/bin/env python3
"""
Local, parallel driver for running katydid over spec-sims output.

Builds a single dataframe of every (subrun, field_index, acquisition) unit
of work by globbing the .speck files produced by local_spec_sims.py, then
either prints the exact katydid command line for each matching row
(--dry_run, or any of --subrun_id/--field_index/--acquisition, which imply
it), or dispatches jobs -- grouped by --parallelize-fields/
--parallelize-acquisitions -- across a flat process pool, entirely within
one `bazel run` invocation.

No persisted state: every invocation rebuilds the full file list from
what's actually on disk and reruns unconditionally -- no checkpoint CSV,
no skip-if-output-exists.

Reused, near-verbatim, from run_ssa_katydid.py: the .speck glob-and-group
logic (create_base_file_df/aggregate_paths), get_slope's physics
calculation, the frequency-acceptance/time-gap-tolerance formula, and the
katydid command-line construction. NOT reused: machine_path hardcoding
(replaced with explicit --runs_base_dir/--katydid_output_dir),
noise-run-id database lookup (replaced with explicit --noise_paths),
base-config-directory indirection (replaced with an explicit
--katydid_config path), the checkpoint/cleanup dataframe branch, and
apptainer/sbatch wrapping.

Example:
    bazel run --@pypi//venv=dev //:local_ssa_katydid -- \\
        --run_name=test1 \\
        --runs_base_dir=/path/to/creswork/sims/runs \\
        --katydid_output_dir=/path/to/creswork/katydid_analysis/root_files \\
        --num_subruns=25 \\
        --katydid_config=/path/to/2-12_LTF_MBEB_tausnr7_2400.yaml \\
        --noise_paths /path/to/noise_ch0.spec /path/to/noise_ch1.spec \\
        --analysis_id=1 \\
        --max-jobs=16 \\
        --parallelize-fields --parallelize-acquisitions

Python version note: this file is written to be compatible with Python 3.9
(e.g. `typing.Optional[int]` instead of the 3.10+ `int | None` syntax),
for the same reason as local_spec_sims.py -- the CENPA venv this may
eventually also run in is 3.9, even though this repo's Bazel build
currently targets a newer version.
"""

import argparse
import re
import subprocess as sp
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob
from pathlib import Path

import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc
import numpy as np
import pandas as pd
import yaml
from python.runfiles import runfiles

KATYDID_RLOCATION = "katydid+/Source/Executables/Main/Katydid"

SPECK_FILENAME_RE = re.compile(r"(\d+)_(\d+)\.speck$")
FIELD_DIR_RE = re.compile(r"(\d+)_field_")


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-r", "--run_name", type=str, required=True, help="spec_sims run_name to run katydid on")
    arg(
        "-rb",
        "--runs_base_dir",
        type=str,
        required=True,
        help="base directory containing runs_base_dir/run_name/subrun_*/*/spec_files/*.speck (i.e. local_spec_sims.py's --runs_base_dir)",
    )
    arg(
        "-ob",
        "--katydid_output_dir",
        type=str,
        required=True,
        help="base directory katydid .root/_SlewTimes.txt files are written under (a run_name/aid_N subdirectory is created within it)",
    )
    arg("-n", "--num_subruns", type=int, required=True, help="number of subruns to look for, 0..num_subruns-1")
    arg("-kc", "--katydid_config", type=str, required=True, help="full path to the katydid yaml config file")
    arg(
        "-np",
        "--noise_paths",
        type=str,
        nargs=2,
        required=True,
        metavar=("CHANNEL_0_PATH", "CHANNEL_1_PATH"),
        help="paths to the two (per-channel) noise .spec(k) files",
    )
    arg("-aid", "--analysis_id", type=int, required=True, help="analysis_id used to label output directories")
    arg(
        "--max-jobs",
        dest="max_jobs",
        type=int,
        default=None,
        help="max number of jobs to run concurrently (default: os.cpu_count())",
    )
    arg(
        "--parallelize-fields",
        dest="parallelize_fields",
        action="store_true",
        help="give each field its own job, instead of bundling all of a subrun's fields into one job",
    )
    arg(
        "--parallelize-acquisitions",
        dest="parallelize_acquisitions",
        action="store_true",
        help="give each acquisition its own job, instead of bundling all of a subrun's (or subrun/field's) acquisitions into one job",
    )
    arg(
        "-d",
        "--dry_run",
        action="store_true",
        help="print the katydid command for every matching row without running it",
    )
    arg(
        "--subrun_id",
        type=int,
        default=None,
        help="dry-run filter: only this subrun_id. Implies --dry_run.",
    )
    arg(
        "--field_index",
        type=int,
        default=None,
        help="dry-run filter: only this field_index. Implies --dry_run.",
    )
    arg(
        "--acquisition",
        type=int,
        default=None,
        help="dry-run filter: only this acquisition. Implies --dry_run.",
    )

    args = par.parse_args()
    if args.subrun_id is not None or args.field_index is not None or args.acquisition is not None:
        args.dry_run = True
    return args


def get_slope(true_field: float, frequency: float = 19.15e9) -> float:
    """Reused verbatim from run_ssa_katydid.py."""
    approx_power = sc.power_larmor(true_field, frequency)
    approx_energy = sc.freq_to_energy(frequency, true_field)
    approx_slope = sc.df_dt(approx_energy, true_field, approx_power)
    return approx_slope


# The only Katydid processor type, anywhere in its own source tree, whose
# Configure() reads a "set-field" key (KTMultiBandEventBuilder.cc) --
# confirmed by grepping Source/ for every file that references "set-field"
# at all: exactly one. If Katydid ever adds another processor type that
# also reads this key, add its own type string here too.
KATYDID_SET_FIELD_PROCESSOR_TYPES = frozenset({"multi-band-event-builder"})


def render_katydid_config(base_config_path: str, true_field: float, output_path: Path) -> None:
    """Writes a copy of base_config_path to output_path with set-field
    written -- creating it if it wasn't already present -- into the config
    block of every processor *instance* whose declared type is one that
    Katydid's own source confirms actually reads that key (see
    KATYDID_SET_FIELD_PROCESSOR_TYPES), found via the config's own
    processors: list (each entry's own name: is the key its config block
    lives under elsewhere in the same document). Deliberately not a fixed
    location like "mbeb" (fragile: assumes every config names its
    multi-band-event-builder instance that specific way) and not a blind
    replace of every key literally named "set-field" anywhere in the
    document (risky: could clobber an unrelated key of some other,
    differently-typed processor that happens to share the name) -- this
    targets exactly the processor instances Katydid's own source says
    consume this key, by their declared type, wherever they're named.

    This is the actual fix for a real problem, not a preemptive nicety:
    prior to this, every row in a run shared one static katydid_config
    file, and nothing anywhere in this pipeline ever varied its
    multi-band-event-builder instance's set-field value per row --
    confirmed directly (not assumed) by grepping this file,
    run_ssa_katydid.py, and sbatch_ssa_katydid.py for any reference to
    "set-field" at all (none). Separately, and not fixable from this repo:
    KTMultiBandEventBuilder.cc itself reads the value into fSetField but
    never references it again anywhere in Katydid's own codebase, so even
    a correctly-set value currently has no effect on any computation --
    still worth fixing here regardless, since rendering the field MBEB is
    told about to match the field actually simulated is this repo's own
    responsibility, independent of whether Katydid currently acts on it.
    """
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


def build_slew_root_filename(row: pd.Series, output_dir: str, footer: str) -> str:
    """Reused verbatim (modulo taking output_dir explicitly rather than from
    the row) from run_ssa_katydid.py's build_slew_root_filename."""
    root_path = output_dir + "/"
    root_path += str(row["subrun_id"]) + "_"
    root_path += str(row["true_field"]) + "T_"
    root_path += str(row["acquisition"])
    root_path += footer
    return root_path


def build_file_df_for_subrun(run_name: str, runs_base_dir: str, subrun_id: int, output_dir: str) -> pd.DataFrame:
    """Reused, near-verbatim, from run_ssa_katydid.py's create_base_file_df,
    plus a new field_index column (parsed from the per-field directory name,
    e.g. "3_field_1.92223T" -> 3) that the original never surfaced.
    """
    speck_glob = str(Path(runs_base_dir) / run_name / f"subrun_{subrun_id}" / "*" / "spec_files" / "*.speck")
    speck_files = glob(speck_glob)
    if not speck_files:
        return pd.DataFrame()

    speck_file_paths = [Path(s) for s in speck_files]
    acqs: list[int] = []
    channels: list[int] = []
    field_indices: list[int] = []
    yaml_files: list[str] = []
    for p in speck_file_paths:
        m = SPECK_FILENAME_RE.search(p.name)
        acqs.append(int(m.group(1)))
        channels.append(int(m.group(2)))
        field_dir = p.parents[1]  # e.g. ".../subrun_3/3_field_1.92223T"
        field_m = FIELD_DIR_RE.match(field_dir.name)
        field_indices.append(int(field_m.group(1)))
        yaml_file_path = field_dir.parent / (field_dir.name + ".yaml")
        yaml_files.append(str(yaml_file_path))

    file_df = pd.DataFrame({"rocks_file_path": speck_files})
    file_df["subrun_id"] = subrun_id
    file_df["acquisition"] = acqs
    file_df["channel"] = channels
    file_df["field_index"] = field_indices
    file_df["spec_sims_yaml"] = yaml_files

    file_df = (
        file_df.sort_values("channel")
        .groupby(["spec_sims_yaml", "subrun_id", "field_index", "acquisition"])
        .agg(rocks_file_path=("rocks_file_path", list))
        .reset_index()
    )

    seeds: dict = {}
    true_fields: dict = {}
    trap_currents: dict = {}
    for yaml_config in set(yaml_files):
        with open(yaml_config) as f:
            spec_sim_config_dict = yaml.load(f, Loader=yaml.FullLoader)
        seeds[yaml_config] = spec_sim_config_dict["Settings"]["rand_seed"]
        true_fields[yaml_config] = spec_sim_config_dict["EventBuilder"]["main_field"]
        trap_currents[yaml_config] = spec_sim_config_dict["EventBuilder"]["trap_current"]

    file_df["seed"] = file_df["spec_sims_yaml"].map(seeds)
    file_df["true_field"] = file_df["spec_sims_yaml"].map(true_fields)
    file_df["trap_current"] = file_df["spec_sims_yaml"].map(trap_currents)
    file_df["run_name"] = run_name
    file_df["approx_slope"] = get_slope(file_df["true_field"])

    file_df["root_file_path"] = file_df.apply(lambda row: build_slew_root_filename(row, output_dir, ".root"), axis=1)
    file_df["slew_file_path"] = file_df.apply(
        lambda row: build_slew_root_filename(row, output_dir, "_SlewTimes.txt"), axis=1
    )
    file_df["rendered_katydid_config_path"] = file_df.apply(
        lambda row: build_slew_root_filename(row, output_dir, "_katydid_config.yaml"), axis=1
    )

    return file_df


def build_file_df(args: argparse.Namespace, output_dir: str) -> pd.DataFrame:
    """Builds the combined file_df across every subrun, 0..num_subruns-1."""
    subrun_dfs = [
        build_file_df_for_subrun(args.run_name, args.runs_base_dir, subrun_id, output_dir)
        for subrun_id in range(args.num_subruns)
    ]
    subrun_dfs = [df for df in subrun_dfs if not df.empty]
    if not subrun_dfs:
        return pd.DataFrame()
    return pd.concat(subrun_dfs, ignore_index=True)


def apply_dry_run_filters(file_df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    filtered = file_df
    if args.subrun_id is not None:
        filtered = filtered[filtered["subrun_id"] == args.subrun_id]
    if args.field_index is not None:
        filtered = filtered[filtered["field_index"] == args.field_index]
    if args.acquisition is not None:
        filtered = filtered[filtered["acquisition"] == args.acquisition]
    return filtered


def build_katydid_command(row: pd.Series, katydid_path: str, noise_paths: list[str]) -> list[str]:
    """Reused, with one real change, from run_ssa_katydid.py's run_katydid:
    -c now points at row's own rendered_katydid_config_path (see
    render_katydid_config) instead of a single static config path shared,
    unmodified, by every row."""
    katydid_command_list = [katydid_path, "-c", row["rendered_katydid_config_path"]]

    for i in range(2):
        katydid_command_list.append(f"--spec1.filenames_{i}=" + noise_paths[i])
    for i in range(2):
        katydid_command_list.append(f"--spec2.filenames_{i}=" + row["rocks_file_path"][i])

    # Keep the LTF acceptance area to 45 bins (90 Hz*s) but scale f vs t with
    # slope based on good reconstruction at 0.711T and 2.00T.
    k = 0.1597 * row["approx_slope"] + 9.88e8
    if k <= 0:
        raise ValueError("No real positive solution (k must be > 0)")
    freq_accept = float(np.sqrt(90 * k))
    tgt = float(np.sqrt(90 / k))

    katydid_command_list.append("--long-tr-find.frequency-acceptance=" + str(freq_accept))
    katydid_command_list.append("--long-tr-find.time-gap-tolerance=" + str(tgt))
    katydid_command_list.append("--long-tr-find.initial-slope=" + str(row["approx_slope"]))
    katydid_command_list.append("--long-tr-find.min-slope=" + str(row["approx_slope"] - 1e10))
    katydid_command_list.append("--rtw.output-file=" + row["root_file_path"])
    katydid_command_list.append("--brw.output-file=" + row["root_file_path"])
    katydid_command_list.append("--stv.output-file=" + row["slew_file_path"])
    katydid_command_list.append("--log-level=PROG")

    return katydid_command_list


def resolve_katydid_path() -> str:
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


def _run_one_job(params: dict) -> None:
    """Runs every row assigned to this job, sequentially, in this worker
    process. This is a top-level function (not a closure) so it can be
    pickled and sent to a spawned worker process."""
    rows: list[dict] = params["rows"]
    katydid_path: str = params["katydid_path"]
    katydid_config: str = params["katydid_config"]
    noise_paths: list[str] = params["noise_paths"]

    for row in rows:
        output_dir = Path(row["root_file_path"]).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        render_katydid_config(katydid_config, row["true_field"], Path(row["rendered_katydid_config_path"]))
        command = build_katydid_command(pd.Series(row), katydid_path, noise_paths)
        print(command, flush=True)

        proc = sp.run(command, capture_output=True)
        out = proc.stdout.decode(errors="replace")
        err = proc.stderr.decode(errors="replace")

        root_path = Path(row["root_file_path"])
        root_exists = root_path.is_file()
        root_size = root_path.stat().st_size if root_exists else 0

        if proc.returncode == 0 and root_exists and root_size > 0:
            print(f"subrun {row['subrun_id']} field {row['field_index']} acq {row['acquisition']}: OK")
        else:
            print(
                f"subrun {row['subrun_id']} field {row['field_index']} acq {row['acquisition']}: FAILED "
                f"(returncode={proc.returncode}, root_exists={root_exists}, root_size={root_size})"
            )
            print("katydid stdout (tail 1k):", out[-1000:])
            if err.strip():
                print("katydid stderr (tail 1k):", err[-1000:])


def build_jobs(file_df: pd.DataFrame, args: argparse.Namespace) -> list[dict]:
    group_keys = ["subrun_id"]
    if args.parallelize_fields:
        group_keys.append("field_index")
    if args.parallelize_acquisitions:
        group_keys.append("acquisition")

    jobs: list[dict] = []
    for _, group in file_df.groupby(group_keys, sort=False):
        jobs.append({"rows": group.to_dict("records")})
    return jobs


def main() -> None:
    args: argparse.Namespace = parse_args()

    output_dir = str(Path(args.katydid_output_dir) / args.run_name / f"aid_{args.analysis_id}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    file_df = build_file_df(args, output_dir)
    if file_df.empty:
        print(f"No .speck files found under {args.runs_base_dir}/{args.run_name}/subrun_*/*/spec_files/*.speck")
        sys.exit(1)

    if args.dry_run:
        filtered = apply_dry_run_filters(file_df, args)
        if filtered.empty:
            print("[dry_run] No rows match the given filters.")
            return
        katydid_path = resolve_katydid_path()
        for _, row in filtered.iterrows():
            command = build_katydid_command(row, katydid_path, args.noise_paths)
            print(
                f"[dry_run] subrun={row['subrun_id']} field_index={row['field_index']} acquisition={row['acquisition']} "
                f"(rendered_katydid_config_path not actually written in dry-run mode, same as root/slew output)"
            )
            print(command)
        return

    katydid_path = resolve_katydid_path()
    jobs = build_jobs(file_df, args)
    for job in jobs:
        job["katydid_path"] = katydid_path
        job["katydid_config"] = args.katydid_config
        job["noise_paths"] = args.noise_paths

    print(
        f"Running {len(jobs)} job(s) ({len(file_df)} total katydid invocations) "
        f"with max_jobs={args.max_jobs or '(cpu count)'}"
    )

    failures: list[int] = []
    with ProcessPoolExecutor(max_workers=args.max_jobs) as pool:
        futures = {pool.submit(_run_one_job, job): i for i, job in enumerate(jobs)}
        for future in as_completed(futures):
            job_index = futures[future]
            try:
                future.result()
            except Exception as e:
                failures.append(job_index)
                print(f"job {job_index}: FAILED with exception: {e}")
                traceback.print_exc()

    if failures:
        print(f"\n{len(failures)} of {len(jobs)} job(s) failed: {sorted(failures)}")
        sys.exit(1)
    print(f"\nAll {len(jobs)} job(s) completed.")


if __name__ == "__main__":
    main()
