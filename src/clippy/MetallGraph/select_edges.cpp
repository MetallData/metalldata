// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1

#include <metalldata/metall_graph.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <clippy/clippy.hpp>
#include <string>
#include <unordered_set>
#include <boost/json.hpp>
#include <ygm/utility/boost_json.hpp>
#include "utils.hpp"

static const std::string method_name = "select_edges";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name,
                      "Returns information and metadata about edges as JSON"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});
  clip.add_optional<std::unordered_set<boost::json::object>>(
    "series_names",
    "Series names to include (default: none). All series must be edge series.",
    {});
  clip.add_optional<size_t>("limit", "Limit number of rows returned", 1000);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");

  auto limit = clip.get<size_t>("limit");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  std::vector<metalldata::metall_graph::series_name> series_names;

  if (!clip.has_argument("series_names")) {
    series_names = mg.get_edge_series_names();
  } else {
    auto series_obj_vec =
      clip.get<std::vector<boost::json::object>>("series_names");
    auto try_obj_r = metalldata::obj2sn(series_obj_vec);
    if (!try_obj_r) {
      comm.cerr0(try_obj_r.error());
      return -1;
    }
    auto try_obj = try_obj_r.value();
    series_names = {try_obj.begin(), try_obj.end()};
  }
  comm.cerr0() << "series_names size = " << series_names.size() << "\n";
  auto bag_result = mg.select_edges(series_names, where_c, limit);

  if (!bag_result) {
    comm.cerr0(bag_result.error());
    return -1;
  }

  auto bag = bag_result.value();
  comm.barrier();
  std::vector<std::vector<metalldata::metall_graph::data_types>> select_vec;
  // rank 0 here because it's the only one needed - to_return
  // just uses rank0's output.
  bag.gather(select_vec, 0);

  bjsn::array json_maps{};
  json_maps.reserve(limit);

  for (const auto &edge : select_vec) {
    bjsn::object edgemap;
    for (int i = 0; i < edge.size(); ++i) {
      auto sname = series_names.at(i);
      auto sval = edge[i];
      std::visit(
        [&](const auto &val) {
          if constexpr (!std::is_same_v<std::decay_t<decltype(val)>,
                                        std::monostate>) {
            edgemap[sname.qualified()] = val;
          }
        },
        sval);
    }
    json_maps.emplace_back(edgemap);
  }

  clip.to_return(json_maps);
  return 0;
} catch (std::runtime_error e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
