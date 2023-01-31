## Description

MetallData is an HPC platform for interactive data science applications at HPC-scales.  It provides an ecosystem for persistent distributed data structures, including algorithms, interactivity and storage.

Contents:
* MetallFrame: an prototype backend for a distributed and persistent data store that can be used from Python, using clippy. MetallFrame builds on Metall, ygm, clippy, and clippy-cpp. 

## Required

- GCC 9.3 or higher is required.
- MPI (for MPI programs)
- CMake 3.14 or higher


## Download and Build

```bash
git clone https://github.com/LLNL/metalldata.git
cd metalldata
mkdir build
cd build
cmake ../
make
```


## Dependencies

This project depends on the following libraries.
They are downloaded and build automatically using CMake.

If one prefers to use Spack to install the libraries, type `spack install [package name]` and `spack load [package name]` before running CMake.

- Boost C++ Libraries 1.78.0 or higher (available in Spack, always required)
- Metall (available in Spack, always required)
- cereal (tested with v1.3.0, available in Spack, required by YGM)
- YGM (required)
- Clippy-cpp (required)


## License

MetallData is distributed under the MIT license.

All new contributions must be made under the MIT license.

See [LICENSE](LICENSE), [NOTICE](NOTICE), and [COPYRIGHT](COPYRIGHT) for details.

SPDX-License-Identifier: MIT

LLNL-CODE-835154
