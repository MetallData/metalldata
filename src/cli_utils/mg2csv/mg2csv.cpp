// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>
#include <fstream>
#include <string>

/**
 * @brief Dump metall_graph to CSV files
 *
 * This program reads a metall_graph and dumps it to CSV files.
 * When run with MPI, each rank creates its own pair of CSV files:
 *   - <output_prefix>_nodes_rank<N>.csv
 *   - <output_prefix>_edges_rank<N>.csv
 *
 * Usage: mpirun -n <procs> ./mg2csv <metall_graph_path> <output_prefix>
 */

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  // Parse command line arguments
  if (argc < 3) {
    world.cerr0("Usage: ", argv[0], " <metall_graph_path> <output_prefix>");
    world.cerr0("Example: mpirun -n 4 ./mg2csv graph_data output");
    world.cerr0(
      "  Creates: output_nodes_rank0.csv, output_edges_rank0.csv, "
      "etc.");
    return 1;
  }

  std::string metall_path   = argv[1];
  std::string output_prefix = argv[2];

  world.cout0("Opening metall_graph at: ", metall_path);

  try {
    // Open existing metall_graph
    metalldata::metall_graph graph(world, metall_path, false);

    if (!graph.good()) {
      world.cerr0("Error: Failed to open metall_graph at ", metall_path);
      return 1;
    }

    world.cout0("Successfully opened metall_graph");
    world.cout0("Total nodes: ", graph.num_nodes());
    world.cout0("Total edges: ", graph.num_edges());

    // Get series names
    auto node_series_names = graph.get_node_series_names();
    auto edge_series_names = graph.get_edge_series_names();

    // Create output filenames for this rank
    std::string nodes_filename =
      output_prefix + "_nodes_rank" + std::to_string(world.rank()) + ".csv";
    std::string edges_filename =
      output_prefix + "_edges_rank" + std::to_string(world.rank()) + ".csv";

    world.cout0("Rank ", world.rank(), " writing to: ", nodes_filename, " and ",
                edges_filename);

    // Write nodes CSV
    {
      std::ofstream nodes_file(nodes_filename);
      if (!nodes_file.is_open()) {
        world.cerr0("Error: Failed to open output file: ", nodes_filename);
        return 1;
      }

      // Write header
      bool first = true;
      for (const auto& series_name : node_series_names) {
        if (!first) nodes_file << ",";
        nodes_file << series_name;
        first = false;
      }
      nodes_file << "\n";

      // Write data rows
      graph.for_all_nodes(
        [&](auto record_id) {
          bool first = true;
          for (const auto& series_name : node_series_names) {
            if (!first) nodes_file << ",";

            graph.visit_node_field(
              series_name, record_id, [&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, std::string_view>) {
                  nodes_file << "\"" << value << "\"";
                } else {
                  nodes_file << value;
                }
              });
            first = false;
          }
          nodes_file << "\n";
        },
        metalldata::metall_graph::where_clause());

      nodes_file.close();
      world.cout0("Rank ", world.rank(), " wrote nodes to: ", nodes_filename);
    }

    // Write edges CSV
    {
      std::ofstream edges_file(edges_filename);
      if (!edges_file.is_open()) {
        world.cerr0("Error: Failed to open output file: ", edges_filename);
        return 1;
      }

      // Write header
      bool first = true;
      for (const auto& series_name : edge_series_names) {
        if (!first) edges_file << ",";
        edges_file << series_name;
        first = false;
      }
      edges_file << "\n";

      // Write data rows
      graph.for_all_edges(
        [&](auto record_id) {
          bool first = true;
          for (const auto& series_name : edge_series_names) {
            if (!first) edges_file << ",";

            graph.visit_edge_field(
              series_name, record_id, [&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, std::string_view>) {
                  edges_file << "\"" << value << "\"";
                } else {
                  edges_file << value;
                }
              });
            first = false;
          }
          edges_file << "\n";
        },
        metalldata::metall_graph::where_clause());

      edges_file.close();
      world.cout0("Rank ", world.rank(), " wrote edges to: ", edges_filename);
    }

    world.barrier();
    world.cout0("All ranks completed successfully!");

  } catch (const std::exception& e) {
    world.cerr0("Error: ", e.what());
    return 1;
  }

  return 0;
}
