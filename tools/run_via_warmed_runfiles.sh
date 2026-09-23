#!/usr/bin/env bash
# Invokes a py_binary target's own already-warmed-up venv directly: every
# step bazel's own generated launcher performs, minus the one-time venv-
# creation filesystem write, which this assumes has already happened (see
# this repo's own wulf orchestration design: multiple concurrent job
# submitters must never trigger a rebuild while other jobs are running).
# Verified directly against a real, working bazel-generated launcher for
# one target (local_ssa) before being generalized to take the target name
# as a parameter -- not yet re-verified for any other target.
#
# Usage: run_via_warmed_runfiles.sh <target_name> [args...]
#   target_name: a py_binary's own name under //rocks_analysis_pipeline,
#     e.g. "stage1_task" -- must already have been built at least once
#     (bazel build --@pypi//venv=dev //rocks_analysis_pipeline:<target_name>)
#     from this exact checkout; bazel-bin/rocks_analysis_pipeline/
#     <target_name>.runfiles must already exist, sibling to this script's
#     own checkout root.
set -o errexit -o nounset -o pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <target_name> [args...]" >&2
  exit 1
fi
TARGET_NAME="$1"
shift

CHECKOUT_ROOT="$(cd "$(dirname "${BASH_SOURCE}")/.." && pwd)"
RUNFILES_DIR_CANDIDATE="${CHECKOUT_ROOT}/bazel-bin/rocks_analysis_pipeline/${TARGET_NAME}.runfiles"

# Existence only: this can't detect a stale warm-up (source changed since
# the last bazel run, with nobody re-running it) or a venv left partially
# written by an interrupted bazel run -- bazel gives no atomic signal for
# either, short of an explicit marker this repo doesn't record. What this
# does catch: never having been warmed up at all, which would otherwise
# fail deep inside runfiles.bash sourcing or a missing python3, with a much
# less obvious error.
if [[ ! -d "${RUNFILES_DIR_CANDIDATE}" ]]; then
  echo "ERROR: '${TARGET_NAME}' has never been built from this checkout (${RUNFILES_DIR_CANDIDATE} does not exist)." >&2
  exit 1
fi
if [[ ! -d "${RUNFILES_DIR_CANDIDATE}/.${TARGET_NAME}.venv" ]]; then
  echo "ERROR: '${TARGET_NAME}' has been built but never run -- its venv doesn't exist yet. Run once:" >&2
  echo "  bazel run --@pypi//venv=dev //rocks_analysis_pipeline:${TARGET_NAME} -- --help" >&2
  exit 1
fi
export RUNFILES_DIR="${RUNFILES_DIR_CANDIDATE}"

f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR}/$f"

runfiles_export_envvars

PWD="$(pwd)"
function alocation {
  local P=$1
  if [[ "${P:0:1}" == "/" ]]; then
    echo -n "${P}"
  else
    echo -n "${PWD%/}/${P}"
  fi
}

VIRTUAL_ENV="$(alocation "${RUNFILES_DIR}/.${TARGET_NAME}.venv")"
export VIRTUAL_ENV
PATH="${VIRTUAL_ENV}/bin:${PATH}"
export PATH

export BAZEL_TARGET="//rocks_analysis_pipeline:${TARGET_NAME}"
export BAZEL_WORKSPACE="_main"
export BAZEL_TARGET_NAME="${TARGET_NAME}"

if [ -n "${BASH:-}" -o -n "${ZSH_VERSION:-}" ]; then
    hash -r 2> /dev/null
fi

exec "python3" -B -I "$(rlocation "_main/rocks_analysis_pipeline/${TARGET_NAME}.py")" "$@"
