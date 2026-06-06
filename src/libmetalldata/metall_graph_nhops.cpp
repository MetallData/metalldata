// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <string>
#include <variant>
#include <vector>
#include <set>
#include <map>
#include <string_view>
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

metall_graph::return_code metall_graph::nhops(
  const series_name& out_name, size_t nhops,
  const std::vector<std::string>& sources, const where_clause& where) {
  return_code to_return;

  if (!out_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", out_name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(out_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", out_name.qualified());
    return to_return;
  }

  auto u_col_o = m_pedges->find_series(U_COL.unqualified());
  if (!u_col_o.has_value()) {
    to_return.error = std::format("Series {} not found", U_COL.qualified());
    return to_return;
  }
  auto v_col_o = m_pedges->find_series(V_COL.unqualified());
  if (!v_col_o.has_value()) {
    to_return.error = std::format("Series {} not found", V_COL.qualified());
    return to_return;
  }
  auto dir_col_o = m_pedges->find_series(DIR_COL.unqualified());
  if (!dir_col_o.has_value()) {
    to_return.error = std::format("Series {} not found", DIR_COL.qualified());
    return to_return;
  }

  auto u_col = u_col_o.value();
  auto v_col = v_col_o.value();
  auto dir_col = dir_col_o.value();

  // TODO: convert to (rank, node row id) tuples.
  ygm::container::map<std::string, std::vector<std::string>> adj_list(m_comm);

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      auto ouv= priv_local_edge_uv(eid);
      YGM_ASSERT_RELEASE(ouv.has_value());
      std::string u(ouv.value().first);
      std::string v(ouv.value().second);

      
      auto is_directed = priv_local_edge_is_directed(eid);
      auto adj_inserter = [](const std::string&, std::vector<std::string>& adj,
                             const std::string& vert) { adj.push_back(vert); };
      adj_list.async_visit(u, adj_inserter, v);
      if (is_directed.has_value() && !is_directed.value()) {
        adj_list.async_visit(v, adj_inserter, u);
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
    to_return.error = error;
    return to_return;
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

  to_return = set_node_column(out_name, local_nhop_map);

  return to_return;
}
}