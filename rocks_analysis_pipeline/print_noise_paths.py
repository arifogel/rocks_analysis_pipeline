#!/usr/bin/env python3
"""
Resolves and prints the noise file paths for a given noise_id, via
noise_paths.resolve_noise_paths_from_id. Useful for checking what a
noise_id resolves to, or that DB access and the resolved files are
working, without running a full pipeline task.

Example:
    bazel run --@pypi//venv=dev //:print_noise_paths -- --noise-id=1234
"""

import argparse

from rocks_analysis_pipeline.noise_paths import resolve_noise_paths_from_id


def parse_args() -> argparse.Namespace:
    par = argparse.ArgumentParser()
    par.add_argument("--noise-id", type=int, required=True, help="run_id to look up in he6cres_runs.spec_files")
    return par.parse_args()


def main() -> None:
    args = parse_args()
    for path in resolve_noise_paths_from_id(args.noise_id):
        print(path)


if __name__ == "__main__":
    main()
