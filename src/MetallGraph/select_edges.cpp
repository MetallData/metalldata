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
#include <ygm/utility/boost_json.hpp>
#include "utils.hpp"

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
    "series_names", "Series names to include (default: none)", {});

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
    auto e     = mg.get_edge_series_names();
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
  boost::json::array edges_array;

  mg.for_all_edges(
    [&](auto rid) {
      boost::json::object edge_obj;

      for (const auto& series : series_set) {
        // TODO: make this better. This is potentially expensive because we have
        // to do a field lookup on every edge.
        mg.visit_edge_field(series, rid, [&](auto val) {
          using T = std::decay_t<decltype(val)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            edge_obj[series.unqualified()] = std::string(val);
          } else {
            edge_obj[series.unqualified()] = val;
          }
        });
      }

      edges_array.push_back(edge_obj);
    },
    where_c);

  std::vector<bjsn::array> everything(comm.size() - 1);  // don't need rank 0
  static auto&             s_everything = everything;
  comm.cf_barrier();
  if (!comm.rank0()) {
    comm.async(
      0,
      [](const bjsn::array& rank_data, int rank) {
        (s_everything)[rank - 1] = rank_data;
      },
      edges_array, comm.rank());
  }

  comm.barrier();

  if (comm.rank0()) {
    for (auto& el : everything) {
      edges_array.insert(edges_array.end(), el);
      el.clear();
    }
  }

  comm.barrier();
  clip.to_return(edges_array);
  return 0;
}
