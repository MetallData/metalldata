// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1

#include "metall_graph.hpp"
#include <ygm/comm.hpp>
#include <clippy/clippy.hpp>
#include <string>
#include <unordered_set>
#include <boost/json.hpp>

static const std::string method_name    = "select_edges";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name,
                      "Returns edge information and metadata as JSON"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});
  clip.add_optional<std::unordered_set<boost::json::object>>(
    "metadata", "Column names to include", {});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path  = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");
  auto metadata_set =
    clip.get<std::unordered_set<boost::json::object>>("metadata");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  std::unordered_set<metalldata::metall_graph::series_name> metadata;
  if (metadata_set.empty()) {
    auto e   = mg.get_edge_series_names();
    metadata = {e.begin(), e.end()};
  } else {
    metadata.reserve(metadata_set.size());

    for (const auto& md : metadata_set) {
      if (!md.contains("rule")) {
        comm.cerr0("Series name invalid (norule); ignoring");
        continue;
      }
      auto md_rule = md.at("rule");

      auto md_rule_obj = md_rule.get_object();
      if (!md_rule_obj.contains("var")) {
        comm.cerr0("Series name invalid (novar); ignoring");
        continue;
      }
      metalldata::metall_graph::series_name md_ser(
        md_rule_obj["var"].as_string());

      if (!mg.has_edge_series(md_ser)) {
        comm.cerr0("Series name ", md_ser.qualified(), " not found; skipping");
        continue;
      }

      metadata.insert(md_ser);
    }
  }

  // Build array of edge dictionaries
  boost::json::array edges_array;

  mg.for_all_edges(
    [&](auto rid) {
      boost::json::object edge_obj;

      for (const auto& edgename : metadata) {
        mg.visit_edge_field(edgename, rid, [&](auto val) {
          using T = std::decay_t<decltype(val)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            edge_obj[edgename.unqualified()] = std::string(val);
          } else {
            edge_obj[edgename.unqualified()] = val;
          }
        });
      }

      edges_array.push_back(edge_obj);
    },
    where_c);

  // Output the JSON array
  //   comm.cout0() << boost::json::serialize(edges_array) << std::endl;

  clip.to_return(edges_array);
  return 0;
}
