// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metall_graph.hpp>
#include <ygm/comm.hpp>
#include <iostream>
#include <string>

/**
 * @brief Display statistics about a metall_graph
 *
 * This program opens an existing metall_graph and displays:
 * - Node statistics (number of nodes, series names)
 * - Edge statistics (number of edges, series names)
 * - Series data types and sample values
 *
 * Usage: mpirun -n <procs> ./show_metall_graph_stats <path_to_metall_graph>
 */

void print_separator(const std::string& title = "") {
  if (title.empty()) {
    std::cout << std::string(80, '=') << std::endl;
  } else {
    int padding = (80 - title.length() - 2) / 2;
    std::cout << std::string(padding, '=') << " " << title << " "
              << std::string(80 - padding - title.length() - 2, '=')
              << std::endl;
  }
}

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  // Parse command line arguments
  std::string metall_path = "ingestedges";  // Default path
  if (argc > 1) {
    metall_path = argv[1];
  }

  world.cout0("Opening metall_graph at: ", metall_path);

  try {
    // Open existing metall_graph (without overwrite)
    metalldata::metall_graph graph(world, metall_path, false);

    if (!graph.good()) {
      world.cerr0("Error: Failed to open metall_graph at ", metall_path);
      return 1;
    }

    world.cout0("Successfully opened metall_graph");

    // Display general information
    print_separator("METALL GRAPH STATISTICS");
    world.cout0("Path: ", metall_path);

    // Summary
    print_separator("SUMMARY");
    world.cout0("Graph path: ", metall_path);
    world.cout0("Status: ", (graph.good() ? "VALID" : "INVALID"));
    world.cout0("Total nodes: ", graph.num_nodes());
    world.cout0("Total edges: ", graph.num_edges());
    auto node_series = graph.get_node_series_names();
    auto edge_series = graph.get_edge_series_names();
    world.cout0("Node series count: ", node_series.size());
    for (const auto& name : node_series) {
      world.cout0("  - ", name);
    }

    world.cout0("Edge series count: ", edge_series.size());
    for (const auto& name : edge_series) {
      world.cout0("  - ", name);
    }

    // Print series names

    print_separator();

  } catch (const std::exception& e) {
    world.cerr0("Error: ", e.what());
    return 1;
  }

  return 0;
}
