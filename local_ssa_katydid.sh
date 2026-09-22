#!/usr/bin/env bash

set -euo pipefail

_command() {
  set -euxo pipefail
  if [ "${#NOISE_PATHS[@]}" -ne 2 ]; then
    echo "Missing NOISE_PATHS array" >&2
    exit 1
  fi
  time bazel run -c opt --@pypi//venv=dev //:local_ssa_katydid -- \
    --run_name="${RUN_NAME}" \
    --runs_base_dir="${RUNS_DIR}" \
    --katydid_output_dir="${RUN_DIR}/root_files" \
    --num_subruns=25 \
    --katydid_config="${KATYDID_CONFIG}" \
    --noise_paths "${NOISE_PATHS[@]}" \
    --analysis_id=1 \
    --parallelize-fields \
    --parallelize-acquisitions
}
export -f _command

export RUN_DIR="${RUNS_DIR}/${RUN_NAME}"
nohup bash -c _command >& "${RUN_DIR}/local_ssa_katydid.log" &
