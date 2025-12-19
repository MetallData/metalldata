// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

static const std::string method_name    = "erase_edges";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{
    method_name,
    "Erases edges based on where clause or haystack with index series"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<boost::json::value>(
    "series_name", "Name of the series to use as index", "");
  clip.add_optional<boost::unordered_flat_set<std::string>>(
    "erase_list",
    "List of strings to match against `series_name` to determine whether an "
    "edge should be erased",
    {});
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  bool has_where       = clip.has_argument("where");
  bool has_series_name = clip.has_argument("series_name");
  bool has_erase_list  = clip.has_argument("erase_list");
  bool invalid_list    = has_series_name ^ has_erase_list;
  bool any_list        = has_series_name || has_erase_list;

  if (has_where && any_list) {
    comm.cerr0(
      "Invalid combination of options specified: either a where clause OR a "
      "series name/erase list, but not both");
    return -1;
  }
  if (invalid_list) {
    comm.cerr0(
      "Invalid combination of options: both series name and erase list must be "
      "specified.");
    return -1;
  }
  auto path = clip.get_state<std::string>("path");

  metalldata::metall_graph              mg(comm, path, false);
  metalldata::metall_graph::return_code rc;
  if (has_where) {
    auto where = clip.get<boost::json::object>("where");
    metalldata::metall_graph::where_clause where_c;
    if (where.contains("rule")) {
      where_c = metalldata::metall_graph::where_clause(where["rule"]);
    }
    rc = mg.erase_edges(where_c);
  } else {
    boost::json::object series_obj(
      clip.get<boost::json::value>("series_name").as_object());

    if (!series_obj.contains("rule")) {
      comm.cerr0("Series name invalid (norule); aborting");
    }

    auto rule = series_obj["rule"];

    auto rule_obj = rule.get_object();
    if (!rule_obj.contains("var")) {
      comm.cerr0("Series name invalid (novar); aborting");
    }

    auto series_str = std::string(rule_obj["var"].as_string());
    auto series     = metalldata::metall_graph::series_name(series_str);

    auto erase_list =
      clip.get<boost::unordered_flat_set<std::string>>("erase_list");

    rc = mg.erase_edges(series, erase_list);
  }

  if (!rc.good()) {
    comm.cerr0(rc.error);
    return -1;
  }

  return 0;
}
