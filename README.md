## Required

- GCC 8.1 or more (8.3 or more is recommended due to early implementation of the Filesystem library).
- MPI (for MPI programs)
- CMake 3.14 or more

```bash
git clone ssh://git@czgitlab.llnl.gov:7999/metall/metalldata.git
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

- Boost C++ Libraries 1.77.0 or more (available in Spack, always required)
- Metall (available in Spack, always required)
- cereal (tested with v1.3.0, available in Spack, required by YGM)
- YGM (required by distributed-memory programs)
- Clippy-cpp (required by Clippy applications)


## License

MetallData is distributed under the MIT license.

All new contributions must be made under the MIT license.

See [LICENSE](LICENSE), [NOTICE](NOTICE), and [COPYRIGHT](COPYRIGHT) for details.

SPDX-License-Identifier: MIT
