// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <stdexcept>
#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include "utils.hpp"

static const std::string method_name = "nunique";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name,
                      "Returns the number of unique items for each series"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");

  clip.add_optional<std::vector<std::string>>(
    "series_names",
    "Series names to include (default: all). All series must be edge series.",
    {});
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");

  bool all = !clip.has_argument(
    "series_names");  // if we don't specify it at all, include everything

  metalldata::metall_graph mg(comm, path, false);

  std::set<metalldata::metall_graph::series_name>    meta;
  std::vector<metalldata::metall_graph::series_name> edge_inc_fields;
  std::vector<metalldata::metall_graph::series_name> node_inc_fields;

  if (!all) {
    auto inc_fields_str = clip.get<std::vector<std::string>>("series_names");
    for (const auto inc_field : inc_fields_str) {
      auto sn = metalldata::metall_graph::series_name(inc_field);
      if (!mg.has_series(sn)) {
        comm.cerr0() << "Error: series " << sn << " does not exist in graph\n";
        return 1;
      }
      if (sn.is_edge_series()) {
        edge_inc_fields.emplace_back(sn);
      } else {
        node_inc_fields.emplace_back(sn);
      }
    }

  } else {
    edge_inc_fields = mg.get_edge_series_names();
    node_inc_fields = mg.get_node_series_names();
  }

  std::unordered_set<metalldata::metall_graph::series_name> edge_series_set = {
    edge_inc_fields.cbegin(), edge_inc_fields.cend()};

  std::unordered_set<metalldata::metall_graph::series_name> node_series_set = {
    node_inc_fields.cbegin(), node_inc_fields.cend()};

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
    // comm.cerr("Found RULE");
  }

  auto nuniques = mg.nunique_edge(edge_series_set, where_c);
  auto node_nuniques = mg.nunique_node(node_series_set, where_c);

  nuniques.merge(node_nuniques);

  std::map<std::string, size_t> nunique_str;
  for (const auto &[k, v] : nuniques) {
    nunique_str[k.qualified()] = v;
  }
  clip.to_return(nunique_str);
  return 0;
} catch (std::runtime_error e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
