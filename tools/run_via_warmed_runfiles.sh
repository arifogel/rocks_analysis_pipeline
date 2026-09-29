#!/usr/bin/env bash
# Invokes a script against an already-warmed-up py_binary venv directly:
# every step bazel's own generated launcher performs, minus the one-time
# venv-creation filesystem write, which this assumes has already happened
# (see this repo's own wulf orchestration design: multiple concurrent job
# submitters must never trigger a rebuild while other jobs are running).
#
# venv_target_name and script_target_name are independent: a single
# pre-warmed venv can be shared across multiple entry points, so the venv
# that gets activated need not be the same target as the script being run
# against it.
#
# Usage: run_via_warmed_runfiles.sh <venv_target_name> <script_target_name> [args...]
#   venv_target_name: the py_binary target whose own runfiles/venv this
#     activates, e.g. "release_venv".
#   script_target_name: the target under //rocks_analysis_pipeline whose
#     own <name>.py this execs in that venv, e.g. "local_ssa" -- must be
#     reachable via rlocation from venv_target_name's own runfiles (i.e.
#     included, directly or transitively, in its deps/srcs).
#
# RUNFILES_DIR, if already set by the caller (e.g. a release wrapper that
# knows its own on-disk layout), is used as-is; otherwise this derives it
# from a dev bazel-bin checkout, requiring venv_target_name to already have
# been built at least once (bazel build --@pypi//venv=dev
# //rocks_analysis_pipeline:<venv_target_name>) from this exact checkout.
set -o errexit -o nounset -o pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <venv_target_name> <script_target_name> [args...]" >&2
  exit 1
fi
VENV_TARGET_NAME="$1"
SCRIPT_TARGET_NAME="$2"
shift 2

if [[ -z "${RUNFILES_DIR:-}" ]]; then
  CHECKOUT_ROOT="$(cd "$(dirname "${BASH_SOURCE}")/.." && pwd)"
  RUNFILES_DIR="${CHECKOUT_ROOT}/bazel-bin/rocks_analysis_pipeline/${VENV_TARGET_NAME}.runfiles"
fi

# Existence only: this can't detect a stale warm-up (source changed since
# the last bazel run, with nobody re-running it) or a venv left partially
# written by an interrupted bazel run -- bazel gives no atomic signal for
# either, short of an explicit marker this repo doesn't record. What this
# does catch: never having been warmed up at all, which would otherwise
# fail deep inside runfiles.bash sourcing or a missing python3, with a much
# less obvious error.
if [[ ! -d "${RUNFILES_DIR}" ]]; then
  echo "ERROR: '${VENV_TARGET_NAME}' has never been built from this checkout (${RUNFILES_DIR} does not exist)." >&2
  exit 1
fi
if [[ ! -d "${RUNFILES_DIR}/.${VENV_TARGET_NAME}.venv" ]]; then
  echo "ERROR: '${VENV_TARGET_NAME}' has been built but never run -- its venv doesn't exist yet. Run once:" >&2
  echo "  bazel run --@pypi//venv=dev //rocks_analysis_pipeline:${VENV_TARGET_NAME} -- --help" >&2
  exit 1
fi
export RUNFILES_DIR

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

VIRTUAL_ENV="$(alocation "${RUNFILES_DIR}/.${VENV_TARGET_NAME}.venv")"
export VIRTUAL_ENV
PATH="${VIRTUAL_ENV}/bin:${PATH}"
export PATH

export BAZEL_TARGET="//rocks_analysis_pipeline:${SCRIPT_TARGET_NAME}"
export BAZEL_WORKSPACE="_main"
export BAZEL_TARGET_NAME="${SCRIPT_TARGET_NAME}"

if [ -n "${BASH:-}" -o -n "${ZSH_VERSION:-}" ]; then
    hash -r 2> /dev/null
fi

exec "python3" -B -I "$(rlocation "_main/rocks_analysis_pipeline/${SCRIPT_TARGET_NAME}.py")" "$@"
