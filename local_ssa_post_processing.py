#!/usr/bin/env python3
"""
Local, parallel post-processing driver, replacing sbatch_ssa_post_processing.py
/ run_ssa_post_processing.py.

Produces the same three outputs (tracks.csv, bands.csv, dmtracks.csv), by
locating whatever local_spec_sims.py and local_ssa_katydid.py actually wrote
to disk, rather than reading the rid_df_*.csv files the original SLURM
pipeline produced (local_ssa_katydid.py doesn't write those).

    tracks.csv     -- from katydid's .root files (local_ssa_katydid.py's
                       output). Reuses local_ssa_katydid.build_file_df() to
                       compute the same root_file_path values that script
                       used to run katydid, rather than re-deriving the path
                       convention independently -- so this always matches
                       whatever local_ssa_katydid.py actually produced, even
                       if its own path convention changes later.
    bands.csv,
    dmtracks.csv   -- MC-truth data from the spec-sims .csv files
                       local_spec_sims.py writes into each subrun/field's own
                       output directory (matching Results.save()'s own path
                       computation: config_path.parent / config_path.stem --
                       identical to local_spec_sims.py's own `output_dir`).

Row order does not matter for any of these three outputs: every row is
self-identifying via explicit columns (run_name/subrun_id/field_index/
acquisition/true_field for tracks.csv, root_file_path -- here meaning the
source bands.csv/dmtracks.csv path, matching the original script's own
column semantics -- for the other two), not by position. This is a
statement about this pipeline's own outputs, not about whatever downstream
tool consumes them -- confirm your own tool doesn't assume a specific row
order before relying on this. Because order genuinely doesn't matter here,
--max-jobs parallelizes the (slower) katydid ROOT-file-reading step.

Example:
    bazel run --@pypi//venv=dev //:local_ssa_post_processing -- \\
        --run_name=test1 \\
        --runs_base_dir=/path/to/runs \\
        --katydid_output_dir=/path/to/runs/run1/test1/root_files \\
        --num_subruns=25 \\
        --analysis_id=1 \\
        --output_dir=/path/to/runs/test1/output \\
        --max-jobs=8

Python version note: written for Python 3.9 compatibility, matching
local_spec_sims.py / local_ssa_katydid.py, for the same reason (the CENPA
venv this may eventually also run in).
"""
import argparse
import sys
import traceback
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from glob import glob
from pathlib import Path
from typing import Any, List, Optional

import numpy as np
import pandas as pd

# Reuses local_ssa_katydid.py's own file-discovery logic directly (see
# BUILD.bazel: this target includes local_ssa_katydid.py in its own srcs,
# the same pattern local_spec_sims.py already uses for run_spec_sims.py),
# rather than re-deriving its path convention independently.
from local_ssa_katydid import build_file_df


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-r", "--run_name", type=str, required=True, help="run name")
    arg(
        "-rb",
        "--runs_base_dir",
        type=str,
        required=True,
        help="local_spec_sims.py's --runs_base_dir (used to locate both its "
        ".speck files -- for computing katydid's root_file_path values the "
        "same way local_ssa_katydid.py did -- and its bands.csv/dmtracks.csv "
        "MC-truth output)",
    )
    arg(
        "-ob",
        "--katydid_output_dir",
        type=str,
        required=True,
        help="local_ssa_katydid.py's --katydid_output_dir",
    )
    arg("-n", "--num_subruns", type=int, required=True, help="number of subruns to look for, 0..num_subruns-1")
    arg("-aid", "--analysis_id", type=int, required=True, help="local_ssa_katydid.py's --analysis_id")
    arg(
        "-o",
        "--output_dir",
        type=str,
        required=True,
        help="where to write tracks.csv, bands.csv, dmtracks.csv, and root_files.csv",
    )
    arg(
        "--max-jobs",
        dest="max_jobs",
        type=int,
        default=None,
        help="max number of concurrent workers for reading .root files (default: os.cpu_count())",
    )
    arg(
        "-d",
        "--dry_run",
        action="store_true",
        help="print what would be read/written without doing it",
    )

    return par.parse_args()


def flat(jaggedarray) -> np.ndarray:
    """Reused verbatim from run_ssa_post_processing.py's Postprocessing.flat."""
    flatarray = np.array([])
    for a in jaggedarray.tolist():
        flatarray = np.append(flatarray, a)
    return flatarray


def build_tracks_for_one_root_file(row: dict) -> pd.DataFrame:
    """Reads a single .root file's MultiBandEvent/fTracks branch into a
    DataFrame, tagged with this row's identifying columns. Adapted from
    run_ssa_post_processing.py's build_bulk_track_params_for_single_file:
    same branch path, same "AsObjects" skip, same flattening -- but tagged
    with local_ssa_katydid.py's own identifying columns (subrun_id,
    field_index, acquisition, true_field) rather than the old pipeline's
    file_id, which has no equivalent here. This is a top-level function
    (not a closure) so it can be pickled and sent to a worker process.
    """
    import uproot  # imported inside the worker, not the parent process

    tracks_df = pd.DataFrame()

    rootfile = uproot.open(row["root_file_path"])
    if "MB-events;1" in rootfile.keys():
        tracks_root = rootfile["MB-events;1"]["MultiBandEvent"]["fTracks"]
        for key, branch in tracks_root.items():
            # Skip object/pointer branches that trigger the "arbitrary pointer" error.
            if branch.interpretation.__class__.__name__ == "AsObjects":
                continue
            tracks_df[key[9:]] = flat(branch.array())

    tracks_df["run_name"] = row["run_name"]
    tracks_df["subrun_id"] = row["subrun_id"]
    tracks_df["field_index"] = row["field_index"]
    tracks_df["acquisition"] = row["acquisition"]
    tracks_df["true_field"] = row["true_field"]
    tracks_df["root_file_path"] = row["root_file_path"]

    return tracks_df.reset_index(drop=True)


def build_tracks_csv(file_df: pd.DataFrame, max_jobs: Optional[int]) -> pd.DataFrame:
    """Reads every existing .root file's tracks in parallel (safe: each
    resulting DataFrame is self-identifying via explicit columns, so
    concatenation order -- which as_completed() does not guarantee --
    doesn't matter). Missing .root files are skipped, matching the original
    script's root_file_exists filter.
    """
    rows = [
        row
        for row in file_df.to_dict("records")
        if Path(row["root_file_path"]).is_file()
    ]
    n_missing = len(file_df) - len(rows)
    if n_missing:
        print(f"{n_missing} of {len(file_df)} expected .root file(s) do not exist yet; skipping them.")
    if not rows:
        return pd.DataFrame()

    dfs: List[pd.DataFrame] = []
    with ProcessPoolExecutor(max_workers=max_jobs) as pool:
        futures: dict[Future, dict] = {
            pool.submit(build_tracks_for_one_root_file, row): row for row in rows
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                dfs.append(future.result())
            except Exception as e:
                print(f"FAILED reading {row['root_file_path']}: {e}")
                traceback.print_exc()

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, axis=0).reset_index(drop=True)


def find_mc_truth_csvs(runs_base_dir: str, run_name: str, filename: str) -> List[str]:
    """Matches Results.save()'s own path computation (config_path.parent /
    config_path.stem), which is identical to local_spec_sims.py's own
    per-(subrun, field) output_dir -- so this glob finds exactly what that
    script wrote, at runs_base_dir/run_name/subrun_*/*/{filename}.
    """
    pattern = str(Path(runs_base_dir) / run_name / "subrun_*" / "*" / filename)
    return glob(pattern)


def build_mc_truth_csv(csv_paths: List[str]) -> pd.DataFrame:
    """Reused, near-verbatim, from run_ssa_post_processing.py's
    write_mc_truth_csvs: same "root_file_path" column name for the *source
    csv's own path* (not a katydid .root file -- matching the original
    script's column semantics exactly, for drop-in compatibility with
    anything already relying on that name)."""
    dfs: List[pd.DataFrame] = []
    for csv_path in csv_paths:
        df = pd.read_csv(csv_path)
        df["root_file_path"] = csv_path
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs).reset_index(drop=True)


def main() -> None:
    args: argparse.Namespace = parse_args()

    katydid_dir = str(Path(args.katydid_output_dir) / args.run_name / f"aid_{args.analysis_id}")

    # build_file_df() takes args.run_name/.runs_base_dir/.num_subruns directly --
    # our own arg names were chosen to match local_ssa_katydid.py's exactly for
    # this reason, so args itself can be passed straight through.
    file_df = build_file_df(args, katydid_dir)
    if file_df.empty:
        print(f"No .speck files found under {args.runs_base_dir}/{args.run_name}/subrun_*/*/spec_files/*.speck")
        sys.exit(1)

    bands_csv_paths = find_mc_truth_csvs(args.runs_base_dir, args.run_name, "bands.csv")
    dmtracks_csv_paths = find_mc_truth_csvs(args.runs_base_dir, args.run_name, "dmtracks.csv")

    if args.dry_run:
        n_root_exists = sum(Path(p).is_file() for p in file_df["root_file_path"])
        print(f"[dry_run] {len(file_df)} expected .root file(s) from katydid_output_dir; {n_root_exists} exist on disk")
        print(f"[dry_run] {len(bands_csv_paths)} bands.csv found under runs_base_dir")
        print(f"[dry_run] {len(dmtracks_csv_paths)} dmtracks.csv found under runs_base_dir")
        print(f"[dry_run] would write tracks.csv, bands.csv, dmtracks.csv, root_files.csv to {args.output_dir}")
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_df.to_csv(output_dir / "root_files.csv")
    print(f"Wrote root_files.csv ({len(file_df)} row(s)).")

    tracks_df = build_tracks_csv(file_df, args.max_jobs)
    tracks_df.to_csv(output_dir / "tracks.csv")
    print(f"Wrote tracks.csv ({len(tracks_df)} row(s), from {tracks_df['root_file_path'].nunique() if not tracks_df.empty else 0} .root file(s)).")

    bands_df = build_mc_truth_csv(bands_csv_paths)
    bands_df.to_csv(output_dir / "bands.csv")
    print(f"Wrote bands.csv ({len(bands_df)} row(s), from {len(bands_csv_paths)} source file(s)).")

    dmtracks_df = build_mc_truth_csv(dmtracks_csv_paths)
    dmtracks_df.to_csv(output_dir / "dmtracks.csv")
    print(f"Wrote dmtracks.csv ({len(dmtracks_df)} row(s), from {len(dmtracks_csv_paths)} source file(s)).")


if __name__ == "__main__":
    main()
