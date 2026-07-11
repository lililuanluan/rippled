#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/.build-csf"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

if [[ "$(uname -s)" == "Darwin" ]]; then
	export CC="$(xcrun --find clang)"
	export CXX="$(xcrun --find clang++)"
	export SDKROOT="$(xcrun --sdk macosx --show-sdk-path)"
fi

if [[ ! -f "${VENV_ACTIVATE}" ]]; then
	python3 -m venv "${REPO_ROOT}/.venv"
fi

# shellcheck disable=SC1090
source "${VENV_ACTIVATE}"

if [[ ! -x "${REPO_ROOT}/.venv/bin/conan" ]]; then
	python3 -m pip install "conan>=2.17"
fi

CONAN_HOME_DIR="$(conan config home)"
conan config install "${REPO_ROOT}/conan/profiles/" \
	-tf "${CONAN_HOME_DIR}/profiles/"
conan remote add --index 0 --force xrplf \
	https://conan.ripplex.io

cd "${SCRIPT_DIR}"
# python3 gen_trace.py G12T14 G12T14_trace.json
# python3 gen_trace.py G53T17 G53T17_trace.json
cd -

JOBS=40

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

cmake --build "${BUILD_DIR}" --config Release --target txset --parallel "${JOBS}"
cmake --build "${BUILD_DIR}" --config Release --target closetime --parallel "${JOBS}"

cd "${REPO_ROOT}"

if [[ -x "${BUILD_DIR}/txset" ]]; then
	echo " === running txset consensus replay ==="
	"${BUILD_DIR}/txset"
fi

if [[ -x "${BUILD_DIR}/closetime" ]]; then
	echo " === running closetime consensus replay ==="
	# "${BUILD_DIR}/closetime"
fi
