# show_metall_graph_stats

A utility program to display statistics about a metall_graph.

## Description

This program opens an existing metall_graph and displays comprehensive statistics including:
- Number of node records and node series
- Number of edge records (directed and undirected) and edge series  
- Complete list of all node series names
- Complete list of all edge series names

## Usage

```bash
# Basic usage with default path
./show_metall_graph_stats

# Specify a custom metall_graph path
./show_metall_graph_stats <path_to_metall_graph>

# With MPI (multi-process)
mpirun -n <num_procs> ./show_metall_graph_stats <path_to_metall_graph>
```

## Examples

```bash
# View stats for the default "ingestedges" graph
./show_metall_graph_stats

# View stats for a custom graph location
./show_metall_graph_stats /path/to/my/graph

# Run with 4 MPI processes
mpirun -n 4 ./show_metall_graph_stats ingestedges
```

## Output

The program displays statistics in several sections:

1. **METALL GRAPH STATISTICS** - General information and path
2. **NODE STATISTICS** - Node record count, series count, and series names
3. **EDGE STATISTICS** - Edge record counts (directed/undirected), series count, and series names
4. **SUMMARY** - Overall statistics summary

## Building

The executable is built as part of the tests:

```bash
cd build
make show_metall_graph_stats
```

## Notes

- The program opens the metall_graph in read-only mode (does not modify)
- Default path is "ingestedges" if no argument is provided
- All series names are read directly from the metall_graph metadata
- Works with both single-process and MPI execution
