// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metall_graph.hpp>
#include <ygm/comm.hpp>
#include <string>
#include <filesystem>

void print_usage(ygm::comm& world, const char* prog_name) {
  world.cerr0("Usage: ", prog_name, " [options]\n");
  world.cerr0("\nRequired:\n");
  world.cerr0("  --graph <path>           Path to metall_graph storage\n");
  world.cerr0("\nDegree computation:\n");
  world.cerr0("  --in-degree <col>        Compute in-degree, store in column <col>\n");
  world.cerr0("  --out-degree <col>       Compute out-degree, store in column <col>\n");
  world.cerr0("  (If both --in-degree and --out-degree are specified, uses degree() function)\n");
  world.cerr0("\nOptional:\n");
  world.cerr0("  --where <jsonlogic>      JSONLogic file for filtering nodes\n");
  world.cerr0("\nExamples:\n");
  world.cerr0("  ", prog_name, " --graph my_graph --in-degree in_deg --out-degree out_deg\n");
  world.cerr0("  ", prog_name, " --graph my_graph --in-degree in_deg --where filter.json\n");
}

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  // Parse arguments
  if (argc < 2) {
    world.cerr0("Error: Missing required arguments\n");
    print_usage(world, argv[0]);
    return 1;
  }

  std::string graph_path;
  std::string in_col;
  std::string out_col;
  std::string where_file;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--graph" && i + 1 < argc) {
      graph_path = argv[++i];
    } else if (arg == "--in-degree" && i + 1 < argc) {
      in_col = argv[++i];
    } else if (arg == "--out-degree" && i + 1 < argc) {
      out_col = argv[++i];
    } else if (arg == "--where" && i + 1 < argc) {
      where_file = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      print_usage(world, argv[0]);
      return 0;
    } else {
      world.cerr0("Unknown argument: ", arg);
      print_usage(world, argv[0]);
      return 1;
    }
  }

  // Validate required arguments
  if (graph_path.empty()) {
    world.cerr0("Error: --graph is required");
    print_usage(world, argv[0]);
    return 1;
  }

  if (in_col.empty() && out_col.empty()) {
    world.cerr0("Error: At least one of --in-degree or --out-degree is required");
    print_usage(world, argv[0]);
    return 1;
  }

  // Check if graph exists
  if (!std::filesystem::exists(graph_path)) {
    world.cerr0("Error: Graph not found: ", graph_path);
    return 1;
  }

  try {
    // Open the graph
    world.cout0("Opening metall_graph at: ", graph_path);
    metalldata::metall_graph graph(world, graph_path, false);

    if (!graph.good()) {
      world.cerr0("Error: Failed to open metall_graph at ", graph_path);
      return 1;
    }

    world.cout0("Successfully opened metall_graph");
    world.cout0("Total nodes: ", graph.order());
    world.cout0("Total edges: ", graph.size());

    // Create where clause
    metalldata::metall_graph::where_clause where;
    if (!where_file.empty()) {
      if (!std::filesystem::exists(where_file)) {
        world.cerr0("Error: Where clause file not found: ", where_file);
        return 1;
      }
      world.cout0("Using where clause from: ", where_file);
      where = metalldata::metall_graph::where_clause(where_file);
    } else {
      world.cout0("No where clause specified (using default)");
    }

    // Compute degrees
    if (!in_col.empty() && !out_col.empty()) {
      // Both specified - use degrees() function
      world.cout0("Computing both in-degree and out-degree using degrees()");
      world.cout0("  In-degree  -> ", in_col);
      world.cout0("  Out-degree -> ", out_col);
      auto result = graph.degrees(in_col, out_col, where);
      if (!result.error.empty()) {
        world.cerr0("Error computing degrees: ", result.error);
        return 1;
      }
      world.cout0("Degree computation complete");
    } else if (!in_col.empty()) {
      // Only in-degree
      world.cout0("Computing in-degree -> ", in_col);
      auto result = graph.in_degree(in_col, where);
      if (!result.error.empty()) {
        world.cerr0("Error computing in-degree: ", result.error);
        return 1;
      }
      world.cout0("In-degree computation complete");
    } else if (!out_col.empty()) {
      // Only out-degree
      world.cout0("Computing out-degree -> ", out_col);
      auto result = graph.out_degree(out_col, where);
      if (!result.error.empty()) {
        world.cerr0("Error computing out-degree: ", result.error);
        return 1;
      }
      world.cout0("Out-degree computation complete");
    }

    world.cout0("\nDegree computation successful!");
    world.cout0("Node series available:");
    for (const auto& series : graph.get_node_series_names()) {
      world.cout0("  - ", series);
    }

  } catch (const std::exception& e) {
    world.cerr0("Error: ", e.what());
    return 1;
  }

  return 0;
}
