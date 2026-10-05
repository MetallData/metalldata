#!/usr/bin/env bash
# Reproduces the steps run by .github/workflows/ci-test.yml inside the devcontainer.
set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${WORKSPACE_DIR}/build"

cd "${WORKSPACE_DIR}"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

cmake .. \
    -DMETALLDATA_BUILD_TESTS=ON \
    -DBOOST_FETCH_URL=https://archives.boost.io/release/1.87.0/source/boost_1_87_0.tar.bz2

make -j"$(nproc)"
make test

# end-to-end (clippy/pytest) testing
python3 -m pip install --break-system-packages -r ../test/clippy/requirements.txt -r ../test/clippy/requirements-dev.txt
make pytest
