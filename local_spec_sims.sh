#!/usr/bin/env bash

set -euo pipefail

_command() {
  set -euxo pipefail
  bazel run -c opt --@pypi//venv=dev //:local_spec_sims -- \
    --run_name="${RUN_NAME}" \
    --noise_run_id=1716 \
    --yaml_config="${YAML_CONFIG}" \
    --json_config="${JSON_CONFIG}" \
    --num_subruns=25 \
    --runs_base_dir="${RUNS_DIR}"
}
export -f _command

export RUN_DIR="${RUNS_DIR}/${RUN_NAME}"
mkdir -p "${RUN_DIR}"
nohup bash -c _command >& "${RUN_DIR}/local_spec_sims.log" &
