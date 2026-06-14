// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// TODO: we could probably implement this with a counting set instead
// of a map.

#include <string>
#include <utility>
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

result<> metall_graph::out_degree(series_name                       out_name,
                                  const metall_graph::where_clause& where) {
  return priv_in_out_degree(out_name, where, true);
}

result<> metall_graph::in_degree(series_name                       in_name,
                                 const metall_graph::where_clause& where) {
  return priv_in_out_degree(in_name, where, false);
}

/**
 * @brief Private helper function for computing in-degree or out-degree.
 *
 * This is an internal helper used by in_degree() and out_degree() to
 * calculate degree values for nodes matching a where clause.
 *
 * @param series_name Name of the series to store degree values
 * @param where Where clause to filter nodes
 * @param outdeg If true, compute out-degree; if false, compute in-degree
 * @return result<void>
 */
result<> metall_graph::priv_in_out_degree(
  series_name name, const metall_graph::where_clause& where, bool outdeg) {
  using record_id_type = record_store_type::record_id_type;

  edge_series_idx_type degcol, otherdegcol;
  if (outdeg) {
    degcol = m_u_col_idx;
    otherdegcol = m_v_col_idx;
  } else {
    degcol = m_v_col_idx;
    otherdegcol = m_u_col_idx;
  }

  if (!name.is_node_series()) {
    return std::unexpected(
      std::format("invalid series name: {}", name.qualified()));
  }

  if (m_pnodes->contains_series(name.unqualified())) {
    return std::unexpected(
      std::format("series {} already exists", name.qualified()));
  }

  auto                                      edges_ = m_pedges;
  ygm::container::map<std::string, int64_t> degrees(m_comm);

  std::vector<std::string> nodes;
  priv_for_all_nodes(
    [&](local_node_idx_type nid) {
      auto node_name_o = pl_get_node_label(nid);
      YGM_ASSERT_RELEASE(node_name_o.has_value());
      std::string_view node_name = node_name_o.value();

      degrees.async_insert(std::string(node_name), 0);
    },
    where);

  m_comm.barrier();
  // ygm::container::counting_set<std::string> found_degrees(m_comm, nodes);
  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto edge_name_o = pl_get_edge_field<std::string_view>(degcol, eid);
      YGM_ASSERT_RELEASE(edge_name_o.has_value());
      std::string_view edge_name = edge_name_o.value();
      degrees.async_visit(std::string(edge_name),
                          [](const auto& key, auto& val) { val++; });
      // for undirected edges, add the reverse.
      bool is_directed = pl_edge_is_directed(eid).value_or(false);
      if (!is_directed) {
        auto reverseedge_name_o =
          pl_get_edge_field<std::string_view>(otherdegcol, eid);
        YGM_ASSERT_RELEASE(reverseedge_name_o.has_value());
        degrees.async_visit(std::string(reverseedge_name_o.value()),
                            [](const auto& key, auto& val) { val++; });
      }
    },
    where);

  // not strictly required because the subsequent loop over degrees begins
  // with a barrier. But that's spooky action at a distance, so we will be
  // explicit here.
  m_comm.barrier();

  // for (const auto& [node_name, deg_ct] : found_degrees) {
  //   degrees.async_insert_or_assign(node_name, deg_ct);
  // }

  m_comm.barrier();
  return set_node_column(name, degrees);
}

result<> metall_graph::degrees(series_name in_name, series_name out_name,
                               const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;

  if (!in_name.is_node_series()) {
    return std::unexpected(
      std::format("invalid series name: {}", in_name.qualified()));
  }

  if (!out_name.is_node_series()) {
    return std::unexpected(
      std::format("invalid series name: {}", out_name.qualified()));
  }

  if (m_pnodes->contains_series(in_name.unqualified())) {
    return std::unexpected(
      std::format("series {} already exists", in_name.qualified()));
  }
  if (m_pnodes->contains_series(out_name.unqualified())) {
    return std::unexpected(
      std::format("series {} already exists", out_name.qualified()));
  }

  ygm::container::map<std::string, int64_t> indegrees(m_comm);
  ygm::container::map<std::string, int64_t> outdegrees(m_comm);

  priv_for_all_nodes(
    [&](local_node_idx_type nid) {
      auto node_name_o = pl_get_node_label(nid);
      YGM_ASSERT_RELEASE(node_name_o.has_value());
      std::string_view node_name = node_name_o.value();
      indegrees.async_insert(std::string(node_name), 0);
      outdegrees.async_insert(std::string(node_name), 0);
    },
    where);

  m_comm.barrier();

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto uv_o = pl_get_edge_uv_labels(eid);
      YGM_ASSERT_RELEASE(uv_o.has_value());
      std::string in_edge_name(uv_o.value().first);
      std::string out_edge_name(uv_o.value().first);
      indegrees.async_visit(in_edge_name,
                            [&](const auto& key, auto& val) { val++; });

      outdegrees.async_visit(out_edge_name,
                             [&](const auto& key, auto& val) { val++; });

      bool is_directed = pl_edge_is_directed(eid).value_or(false);
      if (!is_directed) {
        indegrees.async_visit(out_edge_name,
                              [&](const auto& key, auto& val) { val++; });

        outdegrees.async_visit(in_edge_name,
                               [&](const auto& key, auto& val) { val++; });
      }
    },
    where);

  // not strictly required because the subsequent loop over degrees begins
  // with a barrier. But that's spooky action at a distance, so we will be
  // explicit here.
  m_comm.barrier();

  // TODO: we want to abstract this to set_node_column because this is a
  // common operation. Make this a private function inside metall_graph.

  // create a node_local map of record id to node value.
  std::map<std::string, local_node_idx_type> node_to_id{};
  priv_for_all_nodes([&](local_node_idx_type nid) {
    auto node_o = pl_get_node_label(nid);
    YGM_ASSERT_RELEASE(node_o.has_value());
    std::string_view node = node_o.value();
    node_to_id[std::string(node)] = nid;
  });

  // create series and store index so we don't have to keep looking it up.
  auto in_deg_idx = m_pnodes->add_series<int64_t>(in_name.unqualified());
  auto out_deg_idx = m_pnodes->add_series<int64_t>(out_name.unqualified());

  // add the values to the degrees series. We are taking advantage of the fact
  // that the node information is local from the degrees shared counting set
  // because it uses the same partitioning scheme as we used when we added the
  // nodes in ingest.

  auto to_return = set_node_column(in_name, indegrees);
  auto to_return2 = set_node_column(out_name, outdegrees);
  to_return.merge_warnings(to_return2);

  return to_return;
}

result<> metall_graph::degrees2(series_name in_name, series_name out_name,
                                const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;

  if (!in_name.is_node_series()) {
    return std::unexpected(
      std::format("invalid series name: {}", in_name.qualified()));
  }

  if (!out_name.is_node_series()) {
    return std::unexpected(
      std::format("invalid series name: {}", out_name.qualified()));
  }

  if (m_pnodes->contains_series(in_name.unqualified())) {
    return std::unexpected(
      std::format("series {} already exists", in_name.qualified()));
  }
  if (m_pnodes->contains_series(out_name.unqualified())) {
    return std::unexpected(
      std::format("series {} already exists", out_name.qualified()));
  }

  auto                                      edges_ = m_pedges;
  ygm::container::counting_set<std::string> indegrees(m_comm);
  ygm::container::counting_set<std::string> outdegrees(m_comm);

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      auto uv_o = pl_get_edge_uv_labels(eid);
      YGM_ASSERT_RELEASE(uv_o.has_value());
      std::string in_edge_name(uv_o.value().first);
      std::string out_edge_name(uv_o.value().second);
      indegrees.async_insert(in_edge_name);
      outdegrees.async_insert(out_edge_name);

      auto is_directed = pl_edge_is_directed(eid).value_or(false);

      if (!is_directed) {
        indegrees.async_insert(out_edge_name);
        outdegrees.async_insert(in_edge_name);
      }
    },
    where);

  // not strictly required because the subsequent loop over degrees begins
  // with a barrier. But that's spooky action at a distance, so we will be
  // explicit here.
  m_comm.barrier();

  std::map<std::string, int64_t> local_indeg_i64 = {indegrees.begin(),
                                                    indegrees.end()};
  std::map<std::string, int64_t> local_outdeg_i64 = {indegrees.begin(),
                                                     indegrees.end()};
  auto to_return = set_node_column(in_name, local_indeg_i64);

  auto to_return2 = set_node_column(out_name, local_outdeg_i64);

  to_return.merge_warnings(to_return2);

  return to_return;
}

}  // namespace metalldata