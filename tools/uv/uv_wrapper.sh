#!/bin/bash
set -euo pipefail

if [ -z "${BUILD_WORKSPACE_DIRECTORY}" ]; then
    echo "Error: This tool modifies repository files and must be run via 'bazel run'" >&2
    exit 1
fi

_UV_PATH="$(realpath ${1})"
shift

cd "${BUILD_WORKING_DIRECTORY}"
exec "${_UV_PATH}" --directory "${BUILD_WORKSPACE_DIRECTORY}" "$@"

