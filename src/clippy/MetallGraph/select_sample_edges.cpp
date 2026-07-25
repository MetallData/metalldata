// Copyright Lawrence Livermore National Security, LLC and other MetallData
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

static const std::string method_name = "select_sample_edges";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Samples random edges and returns results."};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<size_t>("k", "number of edges to sample");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  clip.add_optional<std::optional<uint64_t>>(
    "seed", "The seed to use for the RNG", std::nullopt);

  clip.add_optional<std::vector<boost::json::object>>(
    "series_names",
    "Series names to include (default: none). All series must be edge series.",
    {});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");

  auto k = clip.get<size_t>("k");
  auto optseed = clip.get<std::optional<uint64_t>>("seed");

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
    series_names = try_obj_r.value();
  }
  auto bag_result = mg.select_sample_edges(k, series_names, optseed, where_c);

  if (!bag_result) {
    comm.cerr0(bag_result.error());
    return -1;
  }

  auto bag = bag_result.value();
  comm.barrier();

  std::vector<std::vector<metalldata::metall_graph::data_types>> select_vec;
  bag.gather(select_vec, 0);

  auto json_maps = rows_to_json(select_vec, series_names);
  clip.to_return(json_maps);

  return 0;
} catch (const std::runtime_error& e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
