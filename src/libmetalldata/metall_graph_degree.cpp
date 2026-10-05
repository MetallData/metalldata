// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// TODO: we could probably implement this with a counting set instead
// of a map.

#include <memory>
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

  if (!name.is_node_series()) {
    return std::unexpected(
      std::format("invalid series name: {}", name.qualified()));
  }

  if (m_pnodes->contains_series(name.unqualified())) {
    return std::unexpected(
      std::format("series {} already exists", name.qualified()));
  }

  auto                                      edges_ = m_pedges;
  ygm::container::map<node_locator, int64_t> degrees(m_comm);
  // static auto*                               sp_degrees = &degrees;

  priv_for_all_nodes(
    [&](local_node_idx_type nid) {
      std::string_view node_name = pl_get_node_label(nid);
      auto             nloc = make_node_locator(m_comm.rank(), nid);

      degrees.async_insert(nloc, 0);
    },
    where);

  m_comm.barrier();
  // ygm::container::counting_set<std::string> found_degrees(m_comm, nodes);
  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto [u, v] = pl_get_edge_uv_locators(eid);
      if (!outdeg) {
        std::swap(u, v);
      }
      degrees.async_visit(u, [](const auto& key, auto& val) { val++; });
      // for undirected edges, add the reverse.
      bool is_directed = pl_edge_is_directed(eid);
      if (!is_directed) {
        degrees.async_visit(v, [](const auto& key, auto& val) { val++; });
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

  return pasync_set_node_column_by_locator(name, degrees);
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

  ygm::container::map<node_locator, int64_t> indegrees(m_comm);
  ygm::container::map<node_locator, int64_t> outdegrees(m_comm);

  priv_for_all_nodes(
    [&](local_node_idx_type nid_l) {
      auto nid = make_node_locator(m_comm.rank(), nid_l);
      indegrees.async_insert(nid, 0);
      outdegrees.async_insert(nid, 0);
    },
    where);

  m_comm.barrier();

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto [u, v] = pl_get_edge_uv_locators(eid);
      indegrees.async_visit(u, [&](const auto& key, auto& val) { val++; });

      outdegrees.async_visit(v, [&](const auto& key, auto& val) { val++; });

      bool is_directed = pl_edge_is_directed(eid);
      if (!is_directed) {
        indegrees.async_visit(u, [&](const auto& key, auto& val) { val++; });

        outdegrees.async_visit(v, [&](const auto& key, auto& val) { val++; });
      }
    },
    where);

  // not strictly required because the subsequent loop over degrees begins
  // with a barrier. But that's spooky action at a distance, so we will be
  // explicit here.
  m_comm.barrier();

  // add the values to the degrees series. We are taking advantage of the fact
  // that the node information is local from the degrees shared counting set
  // because it uses the same partitioning scheme as we used when we added the
  // nodes in ingest.

  auto to_return = pasync_set_node_column_by_locator(in_name, indegrees);
  auto to_return2 = pasync_set_node_column_by_locator(out_name, outdegrees);
  to_return.merge_warnings(to_return2);

  return to_return;
}

// TODO: Decide whether we want to use counting sets for this.
// result<> metall_graph::degrees2(series_name in_name, series_name out_name,
//                                 const metall_graph::where_clause& where) {
//   using record_id_type = record_store_type::record_id_type;

//   if (!in_name.is_node_series()) {
//     return std::unexpected(
//       std::format("invalid series name: {}", in_name.qualified()));
//   }

//   if (!out_name.is_node_series()) {
//     return std::unexpected(
//       std::format("invalid series name: {}", out_name.qualified()));
//   }

//   if (m_pnodes->contains_series(in_name.unqualified())) {
//     return std::unexpected(
//       std::format("series {} already exists", in_name.qualified()));
//   }
//   if (m_pnodes->contains_series(out_name.unqualified())) {
//     return std::unexpected(
//       std::format("series {} already exists", out_name.qualified()));
//   }

//   auto                                      edges_ = m_pedges;
//   ygm::container::counting_set<std::string> indegrees(m_comm);
//   ygm::container::counting_set<std::string> outdegrees(m_comm);

//   priv_for_all_edges(
//     [&](local_edge_idx_type eid) {
//       auto [u, v] = pl_get_edge_uv_labels(eid);
//       std::string in_edge_name(u);
//       std::string out_edge_name(v);
//       indegrees.async_insert(in_edge_name);
//       outdegrees.async_insert(out_edge_name);

//       auto is_directed = pl_edge_is_directed(eid);

//       if (!is_directed) {
//         indegrees.async_insert(out_edge_name);
//         outdegrees.async_insert(in_edge_name);
//       }
//     },
//     where);

//   // not strictly required because the subsequent loop over degrees begins
//   // with a barrier. But that's spooky action at a distance, so we will be
//   // explicit here.
//   m_comm.barrier();

//   std::map<std::string, int64_t> local_indeg_i64 = {indegrees.begin(),
//                                                     indegrees.end()};
//   std::map<std::string, int64_t> local_outdeg_i64 = {indegrees.begin(),
//                                                      indegrees.end()};
//   auto to_return = priv_set_node_series(in_name, local_indeg_i64);

//   auto to_return2 = priv_set_node_series(out_name, local_outdeg_i64);

//   to_return.merge_warnings(to_return2);

//   return to_return;
// }

}  // namespace metalldata
