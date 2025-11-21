# mg2csv - MetallGraph to CSV Converter

## Overview

`mg2csv` is a utility that dumps a metall_graph to CSV files representing nodes and edges with headers. It is MPI-aware and creates separate CSV file pairs for each MPI rank.

## Usage

```bash
mpirun -n <num_procs> ./mg2csv <metall_graph_path> <output_prefix>
```

### Arguments

- `metall_graph_path`: Path to the metall_graph directory
- `output_prefix`: Prefix for output CSV files

### Example

```bash
mpirun -n 4 ./mg2csv /path/to/graph_data output
```

This creates:
- `output_nodes_rank0.csv`, `output_edges_rank0.csv`
- `output_nodes_rank1.csv`, `output_edges_rank1.csv`
- `output_nodes_rank2.csv`, `output_edges_rank2.csv`
- `output_nodes_rank3.csv`, `output_edges_rank3.csv`

## Output Format

### Nodes CSV
- Header row with all node series names
- One row per node record on that rank
- String values are quoted

### Edges CSV
- Header row with all edge series names
- One row per edge record on that rank
- String values are quoted

## Building

The utility is built as part of the main CMake build. From the build directory:

```bash
make mg2csv
```

The executable will be in `build/utils/mg2csv/mg2csv`.

## Notes

- Each MPI rank writes its own local portion of the graph
- The tool opens the metall_graph in read-only mode
- CSV values are comma-separated
- String values are enclosed in double quotes
- All series (columns) are included in the output
