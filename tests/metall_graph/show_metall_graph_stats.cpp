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

    // Node statistics
    print_separator("NODE STATISTICS");
    auto node_series = graph.get_node_series_names();
    world.cout0("Number of node records: ", graph.order());
    world.cout0("Number of node series: ", graph.num_node_series());

    if (node_series.empty()) {
      world.cout0("No node series found");
    } else {
      world.cout0("Node series:");
      for (const auto& series : node_series) {
        world.cout0("  - ", series);
      }
    }

    // Edge statistics
    print_separator("EDGE STATISTICS");

    print_separator("Directed Edges");
    auto directed_edge_series = graph.get_directed_edge_series_names();
    world.cout0("Number of directed edge records: ",
                graph.size(metalldata::metall_graph::EdgeType::DIRECTED));
    world.cout0("Number of directed edge series: ",
                graph.num_directed_edge_series());

    if (directed_edge_series.empty()) {
      world.cout0("No directed edge series found");
    } else {
      world.cout0("Directed edge series:");
      for (const auto& series : directed_edge_series) {
        world.cout0("  - ", series);
      }
    }

    print_separator("Undirected Edges");
    auto undirected_edge_series = graph.get_undirected_edge_series_names();
    world.cout0("Number of undirected edge records: ",
                graph.size(metalldata::metall_graph::EdgeType::UNDIRECTED));
    world.cout0("Number of undirected edge series: ",
                graph.num_undirected_edge_series());

    if (undirected_edge_series.empty()) {
      world.cout0("No undirected edge series found");
    } else {
      world.cout0("Undirected edge series:");
      for (const auto& series : undirected_edge_series) {
        world.cout0("  - ", series);
      }
    }

    // Summary
    print_separator("SUMMARY");
    world.cout0("Graph path: ", metall_path);
    world.cout0("Status: ", (graph.good() ? "VALID" : "INVALID"));
    world.cout0("Total nodes: ", graph.order());
    world.cout0("Total edges: ",
                graph.size(metalldata::metall_graph::EdgeType::DIRECTED |
                           metalldata::metall_graph::EdgeType::UNDIRECTED));
    world.cout0("Node series count: ", node_series.size());
    world.cout0("Directed edge series count: ", directed_edge_series.size());
    world.cout0("Undirected edge series count: ",
                undirected_edge_series.size());

    print_separator();

  } catch (const std::exception& e) {
    world.cerr0("Error: ", e.what());
    return 1;
  }

  return 0;
}
