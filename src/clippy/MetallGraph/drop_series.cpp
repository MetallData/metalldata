// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>

static const std::string method_name = "drop_series";

using series_name = metalldata::metall_graph::series_name;

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Drops a series from a MetallGraph"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<boost::json::object>("series_name",
                                         "The name of the series.");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path     = clip.get_state<std::string>("path");
  auto name_obj = clip.get<boost::json::object>("series_name");

  if (!name_obj.contains("rule")) {
    comm.cerr0("Series name invalid (norule); aborting");
  }

  auto rule = name_obj["rule"];

  auto rule_obj = rule.get_object();
  if (!rule_obj.contains("var")) {
    comm.cerr0("Series name invalid (novar); aborting");
    return 1;
  }

  auto name_str = std::string(rule_obj["var"].as_string());
  auto name     = series_name(name_str);

  metalldata::metall_graph mg(comm, path, false);

  if (!mg.has_series(name)) {
    comm.cerr0("Series name ", name.qualified(), " not found; aborting");
    return 1;
  }

  mg.drop_series(name);
  clip.update_selectors(mg.get_selector_info());

  return 0;
}