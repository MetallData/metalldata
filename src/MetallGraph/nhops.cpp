// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metall_graph.hpp>

static const std::string method_name    = "nhops";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name,
                      "Computes the nhops from a set of seed nodes"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("output", "Output node series name");
  clip.add_required<size_t>("nhops", "Number of hops to compute");
  clip.add_required<std::vector<std::string>>("seeds",
                                              "List of source node ids");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path   = clip.get_state<std::string>("path");
  auto output = clip.get<std::string>("output");
  auto nhops  = clip.get<size_t>("nhops");
  auto seeds  = clip.get<std::vector<std::string>>("seeds");
  auto where  = clip.get<boost::json::object>("where");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
    // comm.cerr("Found RULE");
  }

  metalldata::metall_graph mg(comm, path, false);
  metalldata::metall_graph::series_name sname(output);
  if (sname.prefix().empty()) {
    sname = metalldata::metall_graph::series_name("node", output);
  }
  if (!sname.is_node_series()) {
    comm.cerr0("Invalid node series name: ", sname.qualified());
    return -1;
  }

  auto rc = mg.nhops(sname, nhops, seeds, where_c);

  if (!rc.good()) {
    comm.cerr0(rc.error);
    return -1;
  }

  for (const auto &[warn, count] : rc.warnings) {
    comm.cerr0(std::format("{} : {}", warn, count));
  }

  clip.update_selectors(mg.get_selector_info());
  clip.to_return(0);
  return 0;
}