# pq2mg - Parquet to MetallGraph Converter

## Overview

`pq2mg` is a utility that converts Parquet files containing edge data into metall_graph format. It uses the `ingest_parquet_edges` functionality and is MPI-aware for distributed processing.

## Usage

```bash
mpirun -n <num_procs> ./pq2mg <parquet_file> [options]
```

### Arguments

- `<parquet_file>`: Path to Parquet file with edge data (required)

### Options

- `--col-u <col>`: Column name for source vertex (default: `u`)
- `--col-v <col>`: Column name for target vertex (default: `v`)
- `--directed`: Create directed edges (default: undirected)
- `--meta <cols>`: Comma-separated list of metadata columns to include
- `--recursive`: Read parquet path recursively
- `--output <path>`: Output metall_graph path (default: basename of parquet file)

## Examples

### Basic usage
```bash
# Creates metall_graph named "edges" from edges.parquet
./pq2mg edges.parquet
```

### Custom column names
```bash
# Use "source" and "target" as edge endpoint columns
mpirun -n 4 ./pq2mg edges.parquet --col-u source --col-v target
```

### With metadata and directed edges
```bash
# Include weight and timestamp columns, create directed graph
mpirun -n 4 ./pq2mg edges.parquet --col-u source --col-v target \
  --directed --meta weight,timestamp
```

### Specify output path
```bash
# Create metall_graph at custom path
./pq2mg edges.parquet --output my_graph
```

### Recursive directory reading
```bash
# Read all parquet files in directory recursively
mpirun -n 4 ./pq2mg /path/to/parquet_dir --recursive
```

## Output

The utility creates a metall_graph directory containing:
- Node records (automatically extracted from edge endpoints)
- Edge records with specified metadata
- Default series: `node.id`, `edge.u`, `edge.v`, `edge.directed`
- Additional series for each metadata column specified

## Output Information

The program displays:
- Conversion parameters
- Graph statistics (node count, edge count)
- List of node and edge series created
- Any warnings encountered during ingestion

## Building

The utility is built as part of the main CMake build. From the build directory:

```bash
make pq2mg
```

The executable will be in `build/utils/pq2mg/pq2mg`.

## Notes

- Output metall_graph will overwrite existing graph at the same path
- All MPI ranks must access the same Parquet file(s)
- Metadata columns specified must exist in the Parquet file
- Node records are automatically created from unique edge endpoints
