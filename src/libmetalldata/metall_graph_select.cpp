// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include "ygm/container/bag.hpp"
#include <expected>
#include <utility>
#include <variant>

namespace metalldata {
using metadata_t = std::vector<metall_graph::data_types>;

result<ygm::container::bag<metadata_t>> metall_graph::select_edges(
  const std::vector<metall_graph::series_name>& series_names,
  const metall_graph::where_clause& where, size_t limit) {
  result<ygm::container::bag<metadata_t>> to_return({m_comm});
  ygm::container::bag<metadata_t>&        all_edge_data = to_return.value();
  if (series_names.empty()) {
    return all_edge_data;
  }

  std::vector<metall_graph::edge_series_idx_type> edge_ser_idx;

  for (const auto& s : series_names) {
    if (!s.is_edge_series()) {
      return std::unexpected("all series must be of type edge");
    }
    auto esidx_o = pl_find_edge_series(s.unqualified());
    if (!esidx_o.has_value()) {
      return std::unexpected(std::format("series {} not found", s.qualified()));
    }

    edge_ser_idx.emplace_back(esidx_o.value());
  }

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      metadata_t edge_vals;

      for (const auto& es_idx : edge_ser_idx) {
        auto ser_val =
          pl_get_edge_field(es_idx, eid).value_or(std::monostate{});
        auto count_val = priv_series_to_data_type(ser_val);
        edge_vals.emplace_back(count_val);
      }
      all_edge_data.async_insert(edge_vals);
      if (all_edge_data.local_size() > limit) {
        return;
      }
    },
    where);

  m_comm.barrier();

  return to_return;
}

result<ygm::container::bag<metadata_t>> metall_graph::select_nodes(
  const std::vector<metall_graph::series_name>& series_names,
  const metall_graph::where_clause& where, size_t limit) {
  ygm::container::bag<metadata_t> all_node_data(m_comm);
  if (series_names.empty()) {
    return all_node_data;
  }

  std::vector<std::string>                        warnings;
  std::vector<metall_graph::node_series_idx_type> node_ser_idx;
  for (const auto& s : series_names) {
    if (!s.is_node_series()) {
      return std::unexpected("all series must be of type node");
    }
    auto nsidx_o = pl_find_node_series(s.unqualified());
    if (!nsidx_o.has_value()) {
      return std::unexpected(std::format("series {} not found", s.qualified()));
    }

    node_ser_idx.emplace_back(nsidx_o.value());
  }

  priv_for_all_nodes(
    [&](local_node_idx_type nid) {
      metadata_t node_vals;

      for (const auto& es_idx : node_ser_idx) {
        auto ser_val =
          pl_get_node_field(es_idx, nid).value_or(std::monostate{});
        auto count_val = priv_series_to_data_type(ser_val);
        node_vals.emplace_back(count_val);
      }
      all_node_data.async_insert(node_vals);
      if (all_node_data.local_size() > limit) {
        return;
      }
    },
    where);

  m_comm.barrier();

  return all_node_data;
}

}  // namespace metalldata