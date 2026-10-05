# MetallData Dev Container Guide

This repo ships a VS Code [Dev Container](https://containers.dev) at
[.devcontainer/devcontainer.json](.devcontainer/devcontainer.json) that installs all
MetallData build/runtime dependencies (OpenMPI, clangd/clang-tidy, Boost/CMake toolchain
prerequisites, and the Python packages used by the `clippy` test suite). It gives you a
reproducible environment that matches what [.github/workflows/ci-test.yml](.github/workflows/ci-test.yml)
runs in CI.

## What's installed

See [.devcontainer/Dockerfile](.devcontainer/Dockerfile) — based on
`mcr.microsoft.com/devcontainers/cpp:dev-ubuntu-24.04`, it adds:

- `clangd-19` / `clang-tidy-19`
- `openmpi-bin`, `openmpi-doc`, `libopenmpi-dev`
- `python3-pip`, `python3-venv`, plus `pandas pyarrow faker parquet-tools pytest llnl-clippy
  pytest-cov ruff mypy ipython`
- Env vars: `METALLDATA_BUILD_TESTS=ON`, `CLIPPY_BACKEND_PATH`, `CLIPPY_CMD_PREFIX=mpirun`

## 1. Creating / opening the container

Requirements: Docker Desktop (or compatible engine) running locally, and the VS Code
**Dev Containers** extension (`ms-vscode-remote.remote-containers`).

1. Open the `metalldata` folder in VS Code.
2. Command Palette → **Dev Containers: Reopen in Container**.
   - First run builds the image from [.devcontainer/Dockerfile](.devcontainer/Dockerfile) (a few
     minutes) and starts a container from it.
   - VS Code automatically mounts the repo at `/workspaces/metalldata` and installs the
     extensions listed in `devcontainer.json` (`vscode-clangd`, `cmake-tools`, `doxdocgen`).
3. Subsequent opens reuse the existing container/image (no rebuild) unless the Dockerfile
   or `devcontainer.json` changed.

To force a clean rebuild after editing the Dockerfile:

- Command Palette → **Dev Containers: Rebuild Container** (keeps volumes/mounts, rebuilds image)
- Command Palette → **Dev Containers: Rebuild Without Cache** (also invalidates Docker layer cache)

## 2. Building and running the CI/CD suite locally

Once inside the container (VS Code terminal now runs *in* the container), you can reproduce
the same steps as `.github/workflows/ci-test.yml`:

**One-shot script** (recommended):

```bash
.devcontainer/run-ci.sh
```

This configures CMake with `METALLDATA_BUILD_TESTS=ON`, builds, runs `make test` (ctest), then
installs the `clippy` Python test requirements and runs `make pytest`.

**Or step-by-step**, matching the individual CI steps:

```bash
mkdir -p build && cd build
cmake .. -DMETALLDATA_BUILD_TESTS=ON \
         -DBOOST_FETCH_URL=https://archives.boost.io/release/1.87.0/source/boost_1_87_0.tar.bz2
make -j"$(nproc)"
make test
python3 -m pip install --break-system-packages -r ../test/clippy/requirements.txt -r ../test/clippy/requirements-dev.txt
make pytest
```

**Via VS Code tasks** (Terminal → Run Task…), defined in [.vscode/tasks.json](.vscode/tasks.json):

- `CI: Build and Test (local)` — runs the full script above.
- `Build: Configure (CMake)` / `Build: Make (parallel)` — for iterative development.
- `Test: ctest` / `Test: pytest (clippy)` — run just one test layer.

## 3. Starting, stopping, checkpointing, and resuming the container

VS Code manages the container's lifecycle for you, but it's a regular Docker container
underneath, so the usual `docker` commands work too. Find the container name/id with
`docker ps -a` (Dev Containers names them like `<hash>_metalldata`).

| Action | Via VS Code | Via Docker CLI |
|---|---|---|
| Start / attach | **Dev Containers: Reopen in Container** | `docker start -ai <container>` |
| Stop (keep container + its filesystem state) | Close the VS Code window, or Command Palette → **Dev Containers: Close Remote Connection** | `docker stop <container>` |
| Resume a stopped container | Reopen the folder in VS Code — it reuses the existing (stopped) container rather than recreating it | `docker start <container>` |
| Rebuild from scratch (discard container state, keep image cache) | **Dev Containers: Rebuild Container** | `docker rm <container>` then reopen |
| Rebuild image + container (discard everything) | **Dev Containers: Rebuild Without Cache** | `docker rm <container>` `&&` `docker rmi <image>` then reopen |
| Checkpoint current container state as a reusable image | — | `docker commit <container> metalldata-checkpoint:<tag>` |
| Resume from a checkpointed image | Point `devcontainer.json`'s `"image"` at the checkpoint tag (temporarily, instead of `"build"`) | `docker run -it metalldata-checkpoint:<tag>` |

Notes:

- A build directory created inside `/workspaces/metalldata/build` lives in the repo bind
  mount, so it **persists on your host filesystem** even if the container is removed —
  only container-internal state (e.g. packages installed ad hoc, not via the Dockerfile)
  is lost on `docker rm`.
- Prefer editing [.devcontainer/Dockerfile](.devcontainer/Dockerfile) and rebuilding over manually
  installing packages inside a running container, so the environment stays reproducible and
  is captured in version control.
- Use `docker commit` checkpoints only for ad hoc experimentation; for anything you want to
  keep long-term, fold the change into the Dockerfile instead.
