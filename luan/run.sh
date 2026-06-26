#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/.build-csf"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

if [[ -f "${VENV_ACTIVATE}" ]]; then
  # shellcheck disable=SC1090
  source "${VENV_ACTIVATE}"
fi

JOBS=20

mkdir -p "${BUILD_DIR}"

conan install "${REPO_ROOT}" \
  --output-folder "${BUILD_DIR}" \
  --build missing \
  --settings build_type=Release

cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" \
  -DCMAKE_TOOLCHAIN_FILE:FILEPATH="${BUILD_DIR}/build/generators/conan_toolchain.cmake" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="-DTRACE_TEST" \
  -Dxrpld=OFF \
  -Dtests=OFF \
  -Dcsf=ON

ln -sf "${BUILD_DIR}/compile_commands.json" "${REPO_ROOT}/compile_commands.json"

cmake --build "${BUILD_DIR}" --config Release --target csf --parallel "${JOBS}"

if [[ -x "${BUILD_DIR}/Release/csf" ]]; then
  exec "${BUILD_DIR}/Release/csf"
fi

exec "${BUILD_DIR}/csf"
