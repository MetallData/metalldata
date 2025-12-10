// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>

static const std::string method_name    = "assign";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{
    method_name, "Creates a series and assigns a value based on where clause"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("series_name", "series name to create");
  clip.add_required<metalldata::metall_graph::data_types>("value",
                                                          "value to set");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path     = clip.get_state<std::string>("path");
  auto where    = clip.get<boost::json::object>("where");
  auto name_str = clip.get<std::string>("series_name");
  auto val      = clip.get<metalldata::metall_graph::data_types>("value");

  metalldata::metall_graph::series_name name(name_str);

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  mg.assign(name, val, where_c);
  clip.update_selectors(mg.get_selector_info());
  return 0;
}