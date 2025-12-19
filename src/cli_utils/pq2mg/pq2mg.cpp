// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>
#include <string>
#include <filesystem>
#include <vector>
#include <sstream>

/**
 * @brief Convert Parquet edge data to metall_graph
 *
 * This program reads a Parquet file containing edge data and creates a
 * metall_graph using ingest_parquet_edges. The output metall_graph name
 * is derived from the Parquet file basename.
 *
 * NOTE: We cannot auto-detect and include all Parquet columns by default due to
 * a linker issue. The YGM parquet parser header defines non-inline functions
 * that cause multiple definition errors when included in both the library
 * (metall_graph.cpp) and this executable. As a workaround, users must
 * EXPLICITLY specify metadata columns via --meta.
 *
 * Usage: mpirun -n <procs> ./pq2mg <parquet_file> [options]
 */

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  // Parse command line arguments
  if (argc < 2) {
    world.cerr0("Usage: ", argv[0],
                " <parquet_file> [--col-u <col>] [--col-v <col>] "
                "[--directed] [--meta <col1,col2,...>]");
    world.cerr0("");
    world.cerr0("Arguments:");
    world.cerr0("  <parquet_file>    Path to Parquet file with edge data");
    world.cerr0("");
    world.cerr0("Options:");
    world.cerr0(
      "  --col-u <col>     Column name for source vertex (default: "
      "u)");
    world.cerr0(
      "  --col-v <col>     Column name for target vertex (default: "
      "v)");
    world.cerr0(
      "  --undirected      Create undirected edges (default: "
      "directed)");
    world.cerr0(
      "  --meta <cols>     Comma-separated list of metadata columns "
      "to include (optional)");
    world.cerr0(
      "                    If not specified, only edge endpoints "
      "are stored");
    world.cerr0("  --recursive       Read parquet path recursively");
    world.cerr0(
      "  --output <path>   Output metall_graph path (default: "
      "basename of parquet file)");
    world.cerr0("");
    world.cerr0("Example:");
    world.cerr0(
      "  mpirun -n 4 ./pq2mg edges.parquet --col-u source --col-v "
      "target --undirected");
    return 1;
  }

  std::string              parquet_path = argv[1];
  std::string              col_u        = "u";
  std::string              col_v        = "v";
  bool                     directed     = true;  // Default: directed
  bool                     recursive    = false;
  std::vector<std::string> meta_str;  // Default: empty (no metadata)
  bool                     meta_specified = false;
  std::string              output_path;

  // Parse optional arguments
  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--col-u" && i + 1 < argc) {
      col_u = argv[++i];
    } else if (arg == "--col-v" && i + 1 < argc) {
      col_v = argv[++i];
    } else if (arg == "--undirected") {
      directed = false;
    } else if (arg == "--recursive") {
      recursive = true;
    } else if (arg == "--meta" && i + 1 < argc) {
      meta_specified     = true;
      std::string meta_s = argv[++i];
      // Parse comma-separated metadata columns
      std::stringstream ss(meta_s);
      std::string       item;
      while (std::getline(ss, item, ',')) {
        if (!item.empty()) {
          meta_str.push_back(item);
        }
      }
    } else if (arg == "--output" && i + 1 < argc) {
      output_path = argv[++i];
    } else {
      world.cerr0("Unknown argument: ", arg);
      return 1;
    }
  }

  // Check if parquet file exists
  if (!std::filesystem::exists(parquet_path)) {
    world.cerr0("Error: Parquet file not found: ", parquet_path);
    return 1;
  }

  // Determine output path from parquet file basename if not specified
  if (output_path.empty()) {
    std::filesystem::path p(parquet_path);
    output_path = p.stem().string();  // Get basename without extension
  }

  world.cout0("Converting Parquet to metall_graph:");
  world.cout0("  Input:      ", parquet_path);
  world.cout0("  Output:     ", output_path);
  world.cout0("  Col U:      ", col_u);
  world.cout0("  Col V:      ", col_v);
  world.cout0("  Directed:   ", directed ? "yes" : "no");
  world.cout0("  Recursive:  ", recursive ? "yes" : "no");
  if (!meta_str.empty()) {
    world.cout0("  Metadata:   ", meta_str.size(), " columns");
    for (const auto& m : meta_str) {
      world.cout0("    - ", m);
    }
  } else {
    world.cout0("  Metadata:   None (only edge endpoints)");
  }

  try {
    // Create new metall_graph (overwrite if exists)
    metalldata::metall_graph graph(world, output_path, true);

    if (!graph.good()) {
      world.cerr0("Error: Failed to create metall_graph at ", output_path);
      return 1;
    }

    world.cout0("Successfully created metall_graph");

    // Ingest parquet edges
    world.cout0("Ingesting edges from Parquet file...");

    std::vector<metalldata::metall_graph::series_name> meta;
    meta.reserve(meta_str.size());

    for (const auto& m : meta_str) {
      meta.emplace_back("edge", m);
    }

    auto result = graph.ingest_parquet_edges(parquet_path, recursive, col_u,
                                             col_v, directed, meta);

    if (!result.error.empty()) {
      world.cerr0("Error during ingestion: ", result.error);
      return 1;
    }

    // Print warnings if any
    if (!result.warnings.empty()) {
      world.cout0("Warnings during ingestion:");
      for (const auto& [warning, count] : result.warnings) {
        world.cout0("  [", count, "x] ", warning);
      }
    }

    world.cout0("Ingestion complete!");
    world.cout0("Graph statistics:");
    world.cout0("  Total nodes: ", graph.num_nodes());
    world.cout0("  Total edges: ", graph.num_edges());
    world.cout0("  Node series: ", graph.num_node_series());
    world.cout0("  Edge series: ", graph.num_edge_series());

    world.cout0("\nNode series:");
    for (const auto& series : graph.get_node_series_names()) {
      world.cout0("  - ", series);
    }

    world.cout0("\nEdge series:");
    for (const auto& series : graph.get_edge_series_names()) {
      world.cout0("  - ", series);
    }

    world.cout0("\nSuccess! metall_graph saved to: ", output_path);

  } catch (const std::exception& e) {
    world.cerr0("Error: ", e.what());
    return 1;
  }

  return 0;
}
