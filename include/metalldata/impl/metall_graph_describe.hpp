// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cstddef>
#include <map>
#include <metalldata/metall_graph.hpp>

namespace metalldata {

struct metall_graph::graph_stats {
  size_t                        nv;
  size_t                        ne;
  std::map<series_name, size_t> non_null_edge_ct;
  std::map<series_name, size_t> non_null_node_ct;
};

}  // namespace metalldata
