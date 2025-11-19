// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metall_graph.hpp>
#include <ygm/comm.hpp>
#include <iostream>
#include <string>

/**
 * @brief Test the assign function with optional JSONLogic filtering
 *
 * This program:
 * 1. Loads an existing metall_graph from a user-specified path
 * 2. Adds a new "edge.color" series (string type)
 * 3. Optionally reads a JSONLogic rule from a user-specified file
 * 4. Assigns "blue" to the color column (filtered by JSONLogic if provided)
 *
 * Usage: mpirun -n <procs> ./test_assign <metall_graph_path> [jsonlogic_file]
 */

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  // Parse command line arguments
  if (argc < 2) {
    world.cerr0("Usage: ", argv[0], " <metall_graph_path> [jsonlogic_file]");
    return 1;
  }

  std::string metall_path    = argv[1];
  std::string jsonlogic_file = (argc >= 3) ? argv[2] : "";

  world.cout0("Opening metall_graph at: ", metall_path);

  try {
    // Open existing metall_graph (without overwrite)
    metalldata::metall_graph graph(world, metall_path, false);

    if (!graph.good()) {
      world.cerr0("Error: Failed to open metall_graph at ", metall_path);
      return 1;
    }

    world.cout0("Successfully opened metall_graph");
    world.cout0("Total nodes: ", graph.order());
    world.cout0("Total edges: ", graph.size());

    // Remove the "edge.color" series if it exists
    std::string series_name = "edge.color";
    if (graph.has_edge_series(series_name)) {
      world.cout0("Removing existing series: ", series_name);
      if (!graph.drop_series(series_name)) {
        world.cerr0("Error: Failed to remove series ", series_name);
        return 1;
      }
    }

    // Add the "edge.color" series
    world.cout0("Adding series: ", series_name);

    if (!graph.add_series<std::string_view>(series_name)) {
      world.cerr0("Error: Failed to add series ", series_name);
      return 1;
    }
    world.cout0("Successfully added series: ", series_name);

    // Read JSONLogic rule from file (if provided)
    metalldata::metall_graph::return_code result;
    std::string                           color_value = "blue";

    if (!jsonlogic_file.empty()) {
      world.cout0("Reading JSONLogic rule from: ", jsonlogic_file);

      // Create where_clause from JSONLogic file
      metalldata::metall_graph::where_clause where(jsonlogic_file);

      // Assign "blue" to color column where JSONLogic evaluates to true
      world.cout0("Assigning '", color_value, "' to '", series_name,
                  "' where JSONLogic evaluates to true");

      result = graph.assign(series_name, color_value, where);
    } else {
      // No JSONLogic filter - assign to all edges
      world.cout0("Assigning '", color_value, "' to '", series_name,
                  "' (all edges)");

      result = graph.assign(series_name, color_value);
    }

    if (!result.error.empty()) {
      world.cerr0("Error during assign: ", result.error);
      return 1;
    }

    world.cout0("Successfully assigned values");
    world.cout0("Assignment complete!");

    // Print the first 10 rows of the edge table
    world.cout0("\n=== First 10 rows of edge table ===");
    auto edge_series_names = graph.get_edge_series_names();

    // Print header
    for (const auto& name : edge_series_names) {
      world.cout0() << name << "\t\t";
    }
    world.cout0() << std::endl;

    // Print first 10 edges
    int count = 0;
    graph.for_all_edges(
      [&](auto record_id) {
        if (count >= 10) return;

        // Visit and print each field for this edge
        for (const auto& series_name : edge_series_names) {
          graph.visit_edge_field(series_name, record_id, [](const auto& value) {
            std::cout << value << "\t\t";
          });
        }
        std::cout << std::endl;
        count++;
      },
      metalldata::metall_graph::where_clause());

  } catch (const std::exception& e) {
    world.cerr0("Error: ", e.what());
    return 1;
  }

  return 0;
}
