// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1

#include "metall_graph.hpp"
#include <ygm/comm.hpp>
#include <clippy/clippy.hpp>
#include <string>

static const std::string method_name    = "debug";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

void print_separator(ygm::comm& comm, const std::string& title = "") {
  if (title.empty()) {
    comm.cerr0() << std::string(80, '=') << std::endl;
  } else {
    int padding = (80 - title.length() - 2) / 2;
    comm.cerr0() << std::string(padding, '=') << " " << title << " "
                 << std::string(80 - padding - title.length() - 2, '=')
                 << std::endl;
  }
}

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Provides graph debug information"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<bool>("verbose", "dump all info", false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");

  auto verbose = clip.get<bool>("verbose");

  metalldata::metall_graph mg(comm, path, false);

  print_separator(comm, "METALL GRAPH STATISTICS");
  comm.cerr0("Graph path: ", path);

  print_separator(comm, "SUMMARY");
  comm.cerr0("Status: ", (mg.good() ? "VALID" : "INVALID"));
  comm.cerr0("Total nodes: ", mg.num_nodes());
  comm.cerr0("Total edges: ", mg.num_edges());

  auto node_series = mg.get_node_series_names();
  auto edge_series = mg.get_edge_series_names();
  comm.cerr0("Node series count: ", node_series.size());
  for (const auto& name : node_series) {
    comm.cerr0("  - ", name);
  }

  comm.cerr0("Edge series count: ", edge_series.size());
  for (const auto& name : edge_series) {
    comm.cerr0("  - ", name);
  }

  auto nodenames = mg.get_node_series_names();
  comm.cerr0("nodenames.size() = ", nodenames.size());
  if (verbose) {
    comm.cerr0("Node dump");
    mg.for_all_nodes(
      [&](auto rid) {
        std::stringstream ss;
        ss << "index " << rid << ": ";
        for (const auto& nodename : nodenames) {
          mg.visit_node_field(nodename, rid, [&](auto val) {
            ss << nodename.qualified() << ": " << val << ", ";
          });
        }
        comm.cerr0(ss.str());
      },
      metalldata::metall_graph::where_clause());

    comm.cerr0("Edge dump");
    auto edgenames = mg.get_edge_series_names();
    mg.for_all_edges(
      [&](auto rid) {
        std::stringstream ss;
        ss << "index " << rid << ": ";
        for (const auto& edgename : edgenames) {
          mg.visit_edge_field(edgename, rid, [&](auto val) {
            ss << edgename.qualified() << ": " << val << ", ";
          });
        }
        comm.cerr0(ss.str());
      },
      metalldata::metall_graph::where_clause());
  }
  clip.to_return(0);
  return 0;
}
