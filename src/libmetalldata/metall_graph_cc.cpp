// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <string>
#include <unordered_map>
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
#include <fcntl.h>

#include <boost/graph/graph_traits.hpp>
#include <multiseries/multiseries_record.hpp>
#include <ygm/container/set.hpp>
#include <ygm/container/counting_set.hpp>
#include "metall/tags.hpp"
#include "ygm/utility/assert.hpp"

namespace metalldata {

result<> metall_graph::connected_components(const series_name&  out_name,
                                            const where_clause& where) {
  if (!out_name.is_node_series()) {
    return std::unexpected(
      std::format("Invalid series name: {}", out_name.qualified()));
  }

  if (m_pnodes->contains_series(out_name.unqualified())) {
    return std::unexpected(
      std::format("Series {} already exists", out_name.qualified()));
  }

  // TODO: convert to (rank, node row id) tuples.
  ygm::container::map<node_locator,
                      std::pair<node_locator, std::vector<node_locator>>>
    adj_list(m_comm);

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      auto [u, v] = pl_get_edge_uv_locators(eid);
      bool is_directed = pl_edge_is_directed(eid);
      auto adj_inserter =
        [](const node_locator                                  ccid,
           std::pair<node_locator, std::vector<node_locator>>& adj,
           const node_locator&                                 vert) {
          adj.second.push_back(vert);
          adj.first = ccid;
        };
      adj_list.async_visit(u, adj_inserter, v);
      adj_list.async_visit(v, adj_inserter, u);
    },
    where);
  if (where.is_node_clause()) {
    priv_for_all_nodes_nwhere(
      [&](local_node_idx_type nid) {
        // Do something with each node
        auto nloc = make_node_locator(m_comm.rank(), nid);
        adj_list.async_visit(
          nloc, [](const node_locator&                                 ccid,
                   std::pair<node_locator, std::vector<node_locator>>& adj) {
            adj.first = ccid;
          });
      },
      where);
  }

  static auto* sp_adj_list = &adj_list;
  m_comm.barrier();

  struct cc_visitor {
    void operator()(const node_locator&                                 v,
                    std::pair<node_locator, std::vector<node_locator>>& adj,
                    const node_locator&                                 cc_id) {
      if (cc_id < adj.first) {
        adj.first = cc_id;
        for (const auto& n : adj.second) {
          sp_adj_list->async_visit(n, cc_visitor{}, cc_id);
        }
      }
    }
  };

  adj_list.for_all(
    [&](const node_locator&                                 v,
        std::pair<node_locator, std::vector<node_locator>>& adj) {
      auto min_id = v;
      for (const auto& n : adj.second) {
        min_id = std::min(min_id, n);
      }

      if (min_id == v) {
        for (const auto& n : adj.second) {
          sp_adj_list->async_visit(n, cc_visitor{}, adj.first);
        }
      }
    });

  std::map<local_node_idx_type, std::string>         local_cc_map;
  static std::map<local_node_idx_type, std::string>* sp_local_cc_map = nullptr;
  sp_local_cc_map = &local_cc_map;
  static metall_graph* spthis = nullptr;
  spthis = this;

  //
  // convert locators into local_cc_map
  adj_list.for_all(
    [&](const node_locator&                                 v,
        std::pair<node_locator, std::vector<node_locator>>& adj) {
      adj.second.clear();
      adj.second.shrink_to_fit();
      node_locator ccloc = adj.first;
      auto         move_label = [ccloc, v]() {
        std::string label(spthis->pl_get_node_label(local(ccloc)));
        auto        response = [v](const std::string label) {
          (*sp_local_cc_map)[local(v)] = label;
        };
        spthis->m_comm.async(owner(v), response, label);
      };
      m_comm.async(owner(ccloc), move_label);
    });

  m_comm.barrier();

  // // no warnings possible here, so just return the result directly.
  return priv_set_node_column_by_idx(out_name, local_cc_map);
}

}  // namespace metalldata
