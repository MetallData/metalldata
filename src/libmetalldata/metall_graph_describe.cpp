// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <map>
#include <string>
#include <metalldata/metall_graph.hpp>

namespace metalldata {

result<metall_graph::graph_stats> metall_graph::describe(
  const where_clause &where) {
  result<graph_stats> to_return;
  graph_stats         stats{};

  const auto &[subnode, subedge] = priv_where_subgraph(where);

  stats.nv = subnode.size();
  stats.ne = subedge.size();

  auto subnode_set = std::set(subnode.begin(), subnode.end());
  auto subedge_set = std::set(subedge.begin(), subedge.end());

  auto                              node_series = get_node_series_names();
  std::vector<node_series_idx_type> node_ser_idx;

  for (const auto &s : node_series) {
    auto nsidx_o = pl_find_node_series(s);
    if (!nsidx_o.has_value()) {
      return std::unexpected(std::format("series {} not found", s.qualified()));
    }

    node_ser_idx.emplace_back(nsidx_o.value());
  }

  priv_for_all_nodes(
    [&](const auto &nid) {
      if (!subnode_set.contains(nid)) {
        return;
      }
      auto row = pl_get_node_fields(node_ser_idx, nid);
      for (auto i = 0; i < row.size(); ++i) {
        auto col = row[i];
        if (!col.has_value()) {
          continue;
        }
        if (std::holds_alternative<std::monostate>(col.value())) {
          continue;
        }
        auto series_name = node_series[i];
        stats.non_null_node_ct[series_name]++;
      }
    },
    where);

  auto                              edge_series = get_edge_series_names();
  std::vector<edge_series_idx_type> edge_ser_idx;

  for (const auto &s : edge_series) {
    auto esidx_o = pl_find_edge_series(s);
    if (!esidx_o.has_value()) {
      return std::unexpected(std::format("series {} not found", s.qualified()));
    }

    edge_ser_idx.emplace_back(esidx_o.value());
  }

  priv_for_all_edges(
    [&](const auto &eid) {
      if (!subedge_set.contains(eid)) {
        return;
      }
      auto row = pl_get_edge_fields(edge_ser_idx, eid);
      for (auto i = 0; i < row.size(); ++i) {
        auto col = row[i];
        if (!col.has_value()) {
          continue;
        }
        if (std::holds_alternative<std::monostate>(col.value())) {
          continue;
        }
        auto series_name = edge_series[i];
        stats.non_null_edge_ct[series_name]++;
      }
    },
    where);

  size_t global_nv = ygm::sum(stats.nv, m_comm);
  size_t global_ne = ygm::sum(stats.ne, m_comm);

  auto merge_counts = [](const std::map<series_name, size_t> &a,
                         const std::map<series_name, size_t> &b) {
    auto merged = a;
    for (const auto &[k, v] : b) {
      merged[k] += v;
    }
    return merged;
  };

  auto global_node_fields =
    ygm::all_reduce(stats.non_null_node_ct, merge_counts, m_comm);
  auto global_edge_fields =
    ygm::all_reduce(stats.non_null_edge_ct, merge_counts, m_comm);

  m_comm.barrier();
  if (m_comm.rank0()) {
    stats.nv = global_nv;
    stats.ne = global_ne;
    stats.non_null_node_ct = global_node_fields;
    stats.non_null_edge_ct = global_edge_fields;
    to_return = stats;
  }

  return to_return;
}
}  // namespace metalldata
