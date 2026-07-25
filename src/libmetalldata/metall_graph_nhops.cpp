// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <set>
#include <map>
#include <filesystem>
#include <cassert>
#include <cstdint>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>

#include <metalldata/metall_graph.hpp>
// #include <metall_jl/metall_jl.hpp>
#include <fcntl.h>

#include <boost/graph/graph_traits.hpp>
#include <multiseries/multiseries_record.hpp>
#include <ygm/container/set.hpp>
#include <ygm/container/counting_set.hpp>
#include "metall/tags.hpp"
#include "ygm/utility/assert.hpp"

namespace metalldata {

result<> metall_graph::nhops(const series_name& out_name, size_t nhops,
                             const std::vector<std::string>& sources,
                             const where_clause&             where) {
  if (!out_name.is_node_series()) {
    return std::unexpected(
      std::format("invalid series name: {}", out_name.qualified()));
  }

  if (m_pnodes->contains_series(out_name.unqualified())) {
    return std::unexpected(
      std::format("series {} already exists", out_name.qualified()));
  }

  auto u_col = std::to_underlying(m_u_col_idx);
  auto v_col = std::to_underlying(m_v_col_idx);
  auto dir_col = std::to_underlying(m_dir_col_idx);

  // TODO: convert to (rank, node row id) tuples.
  ygm::container::map<std::string, std::vector<std::string>> adj_list(m_comm);

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      auto [u, v] = pl_get_edge_uv_labels(eid);

      bool is_directed = pl_edge_is_directed(eid);
      auto adj_inserter = [](const std::string&, std::vector<std::string>& adj,
                             const std::string& vert) { adj.push_back(vert); };
      adj_list.async_visit(std::string(u), adj_inserter, std::string(v));
      if (!is_directed) {
        adj_list.async_visit(std::string(v), adj_inserter, std::string(u));
      }
    },
    where);

  std::vector<std::string> missing_vertices;
  for (const auto& source : sources) {
    if (!adj_list.contains(source)) {
      missing_vertices.push_back(source);
    }
  }
  if (!missing_vertices.empty()) {
    std::string error = "source vertex/vertices invalid or missing: ";
    for (size_t i = 0; i < missing_vertices.size(); ++i) {
      if (i > 0) error += ", ";
      error += missing_vertices[i];
    }
    return std::unexpected(error);
  }

  std::map<std::string, int64_t>   local_nhop_map;
  ygm::container::set<std::string> visited(m_comm, sources), cur_level(m_comm),
    next_level(m_comm, sources);
  size_t cur_level_dist = 0;

  static auto* sp_visited = &visited;
  static auto* sp_next_level = &next_level;

  while (next_level.size() > 0 && cur_level_dist <= nhops) {
    cur_level.swap(next_level);
    next_level.clear();
    for (const std::string& v : cur_level) {
      local_nhop_map[v] = static_cast<int64_t>(cur_level_dist);
      if (adj_list.local_count(v) > 0) {
        for (const auto& neighbor : adj_list.local_at(v)) {
          visited.async_contains(neighbor,
                                 [](bool found, const std::string& node) {
                                   if (!found) {
                                     sp_visited->local_insert(node);
                                     sp_next_level->local_insert(node);
                                   }
                                 });
        }
      }
    }

    ++cur_level_dist;
  }

  return set_node_column(out_name, local_nhop_map);
}
}  // namespace metalldata
