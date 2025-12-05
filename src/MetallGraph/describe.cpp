// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metall_graph.hpp>

static const std::string method_name    = "describe";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Provides basic graph statistics"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path  = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  size_t nv = mg.num_nodes(where_c);
  size_t ne = mg.num_edges(where_c);

  clip.to_return(std::make_pair(nv, ne));

  return 0;
}