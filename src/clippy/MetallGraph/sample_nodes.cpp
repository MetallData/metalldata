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

static const std::string method_name    = "sample_nodes";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{
    method_name,
    "Samples random nodes and stores results in a new boolean series."};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>(
    "series_name", "Node series name to store results of selection.");
  clip.add_required<size_t>("k", "number of nodes to sample");
  clip.add_optional<std::optional<uint64_t>>(
    "seed", "The seed to use for the RNG", std::nullopt);

  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path  = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");

  auto k = clip.get<size_t>("k");

  auto optseed = clip.get<std::optional<uint64_t>>("seed");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  auto sn_str = clip.get<std::string>("series_name");
  metalldata::metall_graph::series_name name("node", sn_str);

  metalldata::metall_graph mg(comm, path, false);

  mg.sample_nodes(name, k, optseed, where_c);
  clip.update_selectors(mg.get_selector_info());
  clip.to_return(0);

  return 0;
}
