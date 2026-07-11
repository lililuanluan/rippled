#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/.build-csf"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

usage() {
	echo "Usage: $0 [txset|closetime]" >&2
}

if [[ $# -gt 1 ]]; then
	usage
	exit 2
fi

case "${1:-}" in
"")
	TARGETS=(txset closetime)
	;;
txset | closetime)
	TARGETS=("$1")
	;;
*)
	usage
	exit 2
	;;
esac

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

for target in "${TARGETS[@]}"; do
	case "${target}" in
	txset)
		python3 "${SCRIPT_DIR}/gen_trace.py" \
			"${SCRIPT_DIR}/G53T17" "${SCRIPT_DIR}/G53T17_trace.json"
		;;
	closetime)
		python3 "${SCRIPT_DIR}/gen_trace.py" \
			"${SCRIPT_DIR}/G12T14" "${SCRIPT_DIR}/G12T14_trace.json"
		;;
	esac
done

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

for target in "${TARGETS[@]}"; do
	cmake --build "${BUILD_DIR}" --config Release --target "${target}" --parallel "${JOBS}"
done

cd "${REPO_ROOT}"

for target in "${TARGETS[@]}"; do
	if [[ ! -x "${BUILD_DIR}/${target}" ]]; then
		echo "ERROR: missing replay executable: ${BUILD_DIR}/${target}" >&2
		exit 1
	fi

	echo " === running ${target} consensus replay ==="
	"${BUILD_DIR}/${target}"
done
