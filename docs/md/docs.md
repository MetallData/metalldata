# API Reference

## Table of Contents

- [MetallGraph](#metallgraph)
  - [`__init__`](#__init__)
  - [`add_faker`](#add_faker)
  - [`assign`](#assign)
  - [`describe`](#describe)
  - [`drop_series`](#drop_series)
  - [`dump_parquet_edges`](#dump_parquet_edges)
  - [`dump_parquet_nodes`](#dump_parquet_nodes)
  - [`erase_edges`](#erase_edges)
  - [`ingest_parquet_edges`](#ingest_parquet_edges)
  - [`nhops`](#nhops)
  - [`rename_series`](#rename_series)
  - [`sample_edges`](#sample_edges)
  - [`sample_nodes`](#sample_nodes)
  - [`select_edges`](#select_edges)
  - [`select_nodes`](#select_nodes)
  - [`select_sample_edges`](#select_sample_edges)
  - [`select_sample_nodes`](#select_sample_nodes)
  - [`topk`](#topk)
- [MetallUtils](#metallutils)
  - [`remove`](#remove)
  - [`welcome`](#welcome)

## MetallGraph

A graph data structure

### `__init__`

Initializes a MetallGraph

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `path` | Storage path for MetallGraph | 0 | *required* |
| `overwrite` | Overwrite existing storeage | keyword | `false` |

---

### `add_faker`

Creates a series and assigns fake values based on a faker function

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `series_name` | series name to create | 0 | *required* |
| `generator_type` | type of faker generator (uuid4, integer, double, name, email, etc.) | 1 | *required* |
| `where` | where clause | keyword | `{}` |

---

### `assign`

Creates a series and assigns a value based on where clause

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `series_name` | series name to create | 0 | *required* |
| `value` | value to set | 1 | *required* |
| `where` | where clause | keyword | `{}` |

---

### `describe`

Provides basic graph statistics

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `where` | where clause | keyword | `{}` |

---

### `drop_series`

Drops a series from a MetallGraph

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `series_name` | The name of the series. | 0 | *required* |

---

### `dump_parquet_edges`

Writes a parquet file of edge data

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `output_path` | Path to parquet output | 0 | *required* |
| `fields` | names of series to ingest | keyword | `[]` |
| `overwrite` | If true, overwrite the output file if it exists (default false) | keyword | `false` |

---

### `dump_parquet_nodes`

Writes a parquet file of node data

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `output_path` | Path to parquet output | 0 | *required* |
| `metadata` | Column names of additional fields to ingest | keyword | `[]` |
| `overwrite` | If true, overwrite the output file if it exists (default false) | keyword | `false` |

---

### `erase_edges`

Erases edges based on where clause or haystack with index series

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `erase_list` | List of strings to match against `series_name` to determine whether an edge should be erased | keyword | `[]` |
| `series_name` | Name of the series to use as index | keyword | `""` |
| `where` | where clause | keyword | `{}` |

---

### `ingest_parquet_edges`

Reads a parquet file of edge data

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `input_path` | Path to parquet input | 0 | *required* |
| `col_u` | Edge U column name | 1 | *required* |
| `col_v` | Edge V column name | 2 | *required* |
| `directed` | True if edges are directed (default true) | keyword | `true` |
| `metadata` | Column names of additional fields to ingest | keyword | `[]` |

---

### `nhops`

Computes the nhops from a set of seed nodes

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `output` | Output node series name | 0 | *required* |
| `nhops` | Number of hops to compute | 1 | *required* |
| `seeds` | List of source node ids | 2 | *required* |
| `where` | where clause | keyword | `{}` |

---

### `rename_series`

Renames a series in a MetallGraph

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `old_name` | The series to rename. | 0 | *required* |
| `new_name` | The new name of the series. | 1 | *required* |

---

### `sample_edges`

Samples random edges and stores results in a new boolean series.

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `series_name` | Edge series name to store results of selection. | 0 | *required* |
| `k` | number of edges to sample | 1 | *required* |
| `seed` | The seed to use for the RNG | keyword | `null` |
| `where` | where clause | keyword | `{}` |

---

### `sample_nodes`

Samples random nodes and stores results in a new boolean series.

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `series_name` | Node series name to store results of selection. | 0 | *required* |
| `k` | number of nodes to sample | 1 | *required* |
| `seed` | The seed to use for the RNG | keyword | `null` |
| `where` | where clause | keyword | `{}` |

---

### `select_edges`

Returns information and metadata about edges as JSON

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `limit` | Limit number of rows returned | keyword | `1000` |
| `series_names` | Series names to include (default: none). All series must be edge series. | keyword | `[]` |
| `where` | where clause | keyword | `{}` |

---

### `select_nodes`

Returns information and metadata about nodes as JSON

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `limit` | Limit number of rows returned | keyword | `1000` |
| `series_names` | Series names to include (default: none). All series must be node series. | keyword | `[]` |
| `where` | where clause | keyword | `{}` |

---

### `select_sample_edges`

Samples random edges and returns results.

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `k` | number of edges to sample | 0 | *required* |
| `seed` | The seed to use for the RNG | keyword | `null` |
| `series_names` | Series names to include (default: none). All series must be edge series. | keyword | `[]` |
| `where` | where clause | keyword | `{}` |

---

### `select_sample_nodes`

Samples random nodes and returns results.

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `k` | number of nodes to sample | 0 | *required* |
| `seed` | The seed to use for the RNG | keyword | `null` |
| `series_names` | Series names to include (default: none). All series must be edge series. | keyword | `[]` |
| `where` | where clause | keyword | `{}` |

---

### `topk`

Returns the top k nodes or edges.

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `series` | The series to compare | 0 | *required* |
| `addl_series` | Additional series names to include. Series must be the same type as the `series` parameter. | keyword | `[]` |
| `k` | the number of nodes/edges to return | keyword | `10` |
| `where` | where clause | keyword | `{}` |

---

## MetallUtils

### `remove`

Removes Metall storage across processors

#### Arguments

| Name | Description | Position | Default |
|------|-------------|----------|---------|
| `path` | Path to Metall storage | 0 | *required* |

---

### `welcome`

Prints YGM's welcome message

---

