// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1

#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>
#include <clippy/clippy.hpp>
#include <string>
#include <unordered_set>
#include <boost/json.hpp>
#include <ygm/utility/boost_json.hpp>
#include "utils.hpp"

static const std::string method_name    = "select_nodes";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name,
                      "Returns information and metadata about nodes as JSON"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});
  clip.add_optional<std::unordered_set<boost::json::object>>(
    "series_names",
    "Series names to include (default: none). All series must be node series.",
    {});

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

  std::unordered_set<metalldata::metall_graph::series_name> series_set;
  if (!clip.has_argument("series_names")) {
    auto e     = mg.get_node_series_names();
    series_set = {e.begin(), e.end()};
  } else {
    auto series_obj_set =
      clip.get<std::unordered_set<boost::json::object>>("series_names");
    auto try_obj = metalldata::obj2sn(series_obj_set);
    if (!try_obj.has_value()) {
      comm.cerr0(try_obj.error().error);
      return -1;
    }
    series_set = try_obj.value();
  }

  // Build array of edge dictionaries
  auto expected_array = mg.select_nodes(series_set, where_c);

  if (!expected_array.has_value()) {
    comm.cerr0(expected_array.error());
    return -1;
  }

  clip.to_return(expected_array.value());
  return 0;
}
