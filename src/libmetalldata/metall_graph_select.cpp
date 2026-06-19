// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/detail/collective.hpp>
#include <expected>
#include <utility>
#include <variant>

namespace {
template <typename C>
C down_select(const C& collection, size_t k) {
  C smaller(collection.comm());

  size_t local_count = collection.local_size();
  size_t psum = ygm::prefix_sum(local_count, collection.comm());
  // size_t global_count = ygm::sum(local_count, collection.comm());

  size_t local_k =
    size_t(std::clamp((int(k) - int(psum)), 0, int(local_count)));

  // local_k now holds the number of elements we need to send

  size_t i = 0;
  for (auto it = collection.local_cbegin();
       it != collection.local_cend() && i < local_k; ++it, ++i) {
    smaller.local_insert(*it);
  }

  return smaller;
}
}  // namespace

namespace metalldata {
using metadata_t = std::vector<metall_graph::data_types>;

result<ygm::container::bag<metadata_t>> metall_graph::select_edges(
  const std::vector<metall_graph::series_name>& series_names, size_t limit,
  const metall_graph::where_clause& where) {
  result<ygm::container::bag<metadata_t>> to_return(m_comm);
  ygm::container::bag<metadata_t>&        all_edge_data = to_return.value();
  if (series_names.empty()) {
    return to_return;
  }

  bool limited = (limit != 0);

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

      auto       edge_vals_o = pl_get_edge_fields(edge_ser_idx, eid);
      metadata_t ser_vals;
      for (const auto& v : edge_vals_o) {
        data_types dv = priv_series_to_data_type(v.value_or(std::monostate{}));
        edge_vals.emplace_back(dv);
      }
      all_edge_data.local_insert(edge_vals);
      if (limited && all_edge_data.local_size() > limit) {
        return;
      }
    },
    where);

  if (limited) {
    all_edge_data = down_select(all_edge_data, limit);
  }
  m_comm.barrier();
  return to_return;
}

result<ygm::container::bag<metadata_t>> metall_graph::select_nodes(
  const std::vector<metall_graph::series_name>& series_names, size_t limit,
  const metall_graph::where_clause& where) {
  result<ygm::container::bag<metadata_t>> to_return(m_comm);
  ygm::container::bag<metadata_t>&        all_node_data = to_return.value();
  if (series_names.empty()) {
    return to_return;
  }

  bool limited = (limit != 0);

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

      auto node_vals_o = pl_get_node_fields(node_ser_idx, nid);

      for (const auto& v : node_vals_o) {
        data_types dv = priv_series_to_data_type(v.value_or(std::monostate{}));
        node_vals.emplace_back(dv);
      }
      all_node_data.local_insert(node_vals);
      if (all_node_data.local_size() > limit) {
        return;
      }
    },
    where);

  if (limited) {
    all_node_data = down_select(all_node_data, limit);
  }
  m_comm.barrier();
  return to_return;
}

}  // namespace metalldata