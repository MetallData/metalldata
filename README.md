## Description

MetallData is an HPC platform for interactive data science applications at HPC-scales.  It provides an ecosystem for persistent distributed data structures, including algorithms, interactivity and storage.

Contents:
* MetallGraph: a prototype graph data structure that consists of two dataframes (vertices + edges)

## Required

- GCC 13 or higher is required. 
- MPI (OpenMPI prefered)
- CMake 3.28 or higher


## Download and Build

```bash
git clone https://github.com/LLNL/metalldata.git
cd metalldata
mkdir build
cd build
cmake ../
make
pip install llnl-clippy
```

## License

MetallData is distributed under the MIT license.

All new contributions must be made under the MIT license.

See [LICENSE](LICENSE), [NOTICE](NOTICE), and [COPYRIGHT](COPYRIGHT) for details.

SPDX-License-Identifier: MIT

LLNL-CODE-835154
