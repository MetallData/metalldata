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


metall_graph::return_code metall_graph::out_degree(
  series_name out_name, const metall_graph::where_clause& where) {
  return priv_in_out_degree(out_name, where, true);
}

metall_graph::return_code metall_graph::in_degree(
  series_name in_name, const metall_graph::where_clause& where) {
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
 * @return return_code indicating success or failure
 */
metall_graph::return_code metall_graph::priv_in_out_degree(
  series_name name, const metall_graph::where_clause& where, bool outdeg) {
  using record_id_type = record_store_type::record_id_type;

  metall_graph::return_code to_return;
  series_index_type         degcol, otherdegcol;
  if (outdeg) {
    degcol      = m_u_col_idx;
    otherdegcol = m_v_col_idx;
  } else {
    degcol      = m_v_col_idx;
    otherdegcol = m_u_col_idx;
  }

  if (!name.is_node_series()) {
    to_return.error = std::format("Invalid series name: {}", name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(name.unqualified())) {
    to_return.error = std::format("Series {} already exists", name.qualified());
    return to_return;
  }

  auto                                      edges_ = m_pedges;
  ygm::container::map<std::string, int64_t> degrees(m_comm);

  auto node_col_id = m_pnodes->find_series(NODE_COL.unqualified());

  std::vector<std::string> nodes;
  priv_for_all_nodes(
    [&](record_id_type id) {
      auto node_name_opt = m_pnodes->get<std::string_view>(node_col_id, id);
      YGM_ASSERT_RELEASE(node_name_opt.has_value());
      std::string_view node_name = node_name_opt.value();

      degrees.async_insert(std::string(node_name), 0);
    },
    where);

  m_comm.barrier();
  // ygm::container::counting_set<std::string> found_degrees(m_comm, nodes);
  priv_for_all_edges(
    [&](record_id_type id) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto edge_name_opt = m_pedges->get<std::string_view>(degcol, id);
      YGM_ASSERT_RELEASE(edge_name_opt.has_value());
      std::string_view edge_name = edge_name_opt.value();
      degrees.async_visit(std::string(edge_name),
                          [](const auto& key, auto& val) { val++; });
      // for undirected edges, add the reverse.
      bool is_directed = m_pedges->get<bool>(m_dir_col_idx, id).value_or(false);
      if (!is_directed) {
        auto reverseedge_name_opt =
          m_pedges->get<std::string_view>(otherdegcol, id);
        YGM_ASSERT_RELEASE(reverseedge_name_opt.has_value());
        degrees.async_visit(std::string(reverseedge_name_opt.value()),
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
  set_node_column(name, degrees);

  return to_return;
}

metall_graph::return_code metall_graph::degrees(
  series_name in_name, series_name out_name,
  const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;

  metall_graph::return_code to_return;

  if (!in_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", in_name.qualified());
    return to_return;
  }

  if (!out_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", out_name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(in_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", in_name.qualified());
    return to_return;
  }
  if (m_pnodes->contains_series(out_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", out_name.qualified());
    return to_return;
  }

  ygm::container::map<std::string, int64_t> indegrees(m_comm);
  ygm::container::map<std::string, int64_t> outdegrees(m_comm);

  auto node_col_id = m_pnodes->find_series(NODE_COL.unqualified());

  priv_for_all_nodes(
    [&](record_id_type id) {
      auto node_name_opt = m_pnodes->get<std::string_view>(node_col_id, id);
      YGM_ASSERT_RELEASE(node_name_opt.has_value());
      std::string_view node_name = node_name_opt.value();
      indegrees.async_insert(std::string(node_name), 0);
      outdegrees.async_insert(std::string(node_name), 0);
    },
    where);

  m_comm.barrier();

  auto edges_ = m_pedges;
  priv_for_all_edges(
    [&](record_id_type id) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto in_edge_name_opt = m_pedges->get<std::string_view>(m_v_col_idx, id);
      YGM_ASSERT_RELEASE(in_edge_name_opt.has_value());
      auto in_edge_name = std::string(in_edge_name_opt.value());
      auto out_edge_name_opt = m_pedges->get<std::string_view>(m_u_col_idx, id);
      YGM_ASSERT_RELEASE(out_edge_name_opt.has_value());
      auto out_edge_name = std::string(out_edge_name_opt.value());
      indegrees.async_visit(in_edge_name,
                            [&](const auto& key, auto& val) { val++; });

      outdegrees.async_visit(out_edge_name,
                             [&](const auto& key, auto& val) { val++; });

      bool is_directed = m_pedges->get<bool>(m_dir_col_idx, id).value_or(false);
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
  std::map<std::string, record_id_type> node_to_id{};
  m_pnodes->for_all_rows([&](record_id_type id) {
    auto node_opt = m_pnodes->get<std::string_view>(m_node_col_idx, id);
    YGM_ASSERT_RELEASE(node_opt.has_value());
    std::string_view node = node_opt.value();
    node_to_id[std::string(node)] = id;
  });

  // create series and store index so we don't have to keep looking it up.
  auto in_deg_idx = m_pnodes->add_series<int64_t>(in_name.unqualified());
  auto out_deg_idx = m_pnodes->add_series<int64_t>(out_name.unqualified());

  // add the values to the degrees series. We are taking advantage of the fact
  // that the node information is local from the degrees shared counting set
  // because it uses the same partitioning scheme as we used when we added the
  // nodes in ingest.

  to_return       = set_node_column(in_name, indegrees);
  auto to_return2 = set_node_column(out_name, outdegrees);
  to_return.merge_warnings(to_return2);

  return to_return;
}

metall_graph::return_code metall_graph::degrees2(
  series_name in_name, series_name out_name,
  const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;

  metall_graph::return_code to_return;

  if (!in_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", in_name.qualified());
    return to_return;
  }

  if (!out_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", out_name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(in_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", in_name.qualified());
    return to_return;
  }
  if (m_pnodes->contains_series(out_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", out_name.qualified());
    return to_return;
  }

  auto                                      edges_ = m_pedges;
  ygm::container::counting_set<std::string> indegrees(m_comm);
  ygm::container::counting_set<std::string> outdegrees(m_comm);

  auto u_col   = m_pedges->find_series(U_COL.unqualified());
  auto v_col   = m_pedges->find_series(V_COL.unqualified());
  auto dir_col = m_pedges->find_series(DIR_COL.unqualified());

  priv_for_all_edges(
    [&](record_id_type id) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto in_edge_name_opt = m_pedges->get<std::string_view>(v_col, id);
      YGM_ASSERT_RELEASE(in_edge_name_opt.has_value());
      auto in_edge_name = std::string(in_edge_name_opt.value());

      auto out_edge_name_opt = m_pedges->get<std::string_view>(u_col, id);
      YGM_ASSERT_RELEASE(out_edge_name_opt.has_value());

      auto out_edge_name = std::string(out_edge_name_opt.value());
      indegrees.async_insert(in_edge_name);
      outdegrees.async_insert(out_edge_name);

      auto is_directed = m_pedges->get<bool>(dir_col, id).value_or(false);

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

  to_return       = set_node_column(in_name, indegrees);
  auto to_return2 = set_node_column(out_name, outdegrees);
  to_return.merge_warnings(to_return2);

  return to_return;
}

}