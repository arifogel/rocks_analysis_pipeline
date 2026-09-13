#!/usr/bin/env bash

set -euo pipefail

_command() {
  set -euxo pipefail
  bazel run -c opt --@pypi//venv=dev //:local_ssa_post_processing -- \
    --run_name="${RUN_NAME}" \
    --runs_base_dir="${RUNS_DIR}" \
    --katydid_output_dir="${RUN_DIR}/root_files" \
    --num_subruns=25 \
    --analysis_id=1 \
    --output_dir="${RUN_DIR}/output"
}
export -f _command

export RUN_DIR="${RUNS_DIR}/${RUN_NAME}"
nohup bash -c _command >& "${RUN_DIR}/local_ssa_post_processing.log" &
