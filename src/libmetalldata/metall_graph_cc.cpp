// Copyright Lawrence Livermore National Security, LLC and other MetallData
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
#include <ygm/container/array.hpp>
#include "boost/unordered/unordered_flat_set.hpp"

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
  m_comm.barrier();

  //
  // Count the number of nodes in each connected component
  ygm::container::counting_set<node_locator> cc_sizes(m_comm);
  for (auto& adj : adj_list) {
    adj.second.second.clear();
    adj.second.second.shrink_to_fit();
    cc_sizes.async_insert(adj.second.first);
  }

  //
  // Create sorted array of compoents
  ygm::container::array<std::pair<std::size_t, node_locator>> sorted_cc_sizes(
    m_comm, std::views::transform(cc_sizes, [](const auto& p) {
      return std::make_pair(p.second, p.first);
    }));
  // becsue array::sort is missing a custom comparator, we will sort the array
  // of pairs in reverse order
  sorted_cc_sizes.sort();
  size_t num_components = sorted_cc_sizes.size();

  //
  // Create a map from connected component locator to its rank in the sorted
  // array
  ygm::container::map<node_locator, std::size_t> cc_index_map(m_comm);
  for (const auto& [index, cc] : sorted_cc_sizes) {
    cc_index_map.async_insert(cc.second, num_components - index - 1);
  }
  sorted_cc_sizes.clear();

  //
  // Gather the connected component locators needed by this rank
  boost::unordered::unordered_flat_set<node_locator> cc_locators_i_need;
  for (const auto& adj : adj_list) {
    cc_locators_i_need.insert(adj.second.first);
  }
  auto my_cc_locators = cc_index_map.gather_keys<
    boost::unordered::unordered_flat_map<node_locator, std::size_t>>(
    cc_locators_i_need);
  boost::unordered::unordered_flat_set<node_locator>().swap(cc_locators_i_need);

  //
  // Build output map from node_locator to connected component index
  ygm::container::map<node_locator, std::size_t> cc_index_map_out(m_comm);
  for (const auto& adj : adj_list) {
    cc_index_map_out.async_insert(adj.first,
                                  my_cc_locators.at(adj.second.first));
  }

  //
  // Build final cc map from local node id to connected component index
  std::map<local_node_idx_type, int64_t>         local_cc_map;
  static std::map<local_node_idx_type, int64_t>* sp_local_cc_map = nullptr;
  sp_local_cc_map = &local_cc_map;
  for (const auto& [nl, cc_index] : cc_index_map_out) {
    m_comm.async(
      owner(nl),
      [](local_node_idx_type nid, std::size_t cc_index) {
        (*sp_local_cc_map)[nid] = cc_index;
      },
      local(nl), cc_index);
  }
  m_comm.barrier();

  // no warnings possible here, so just return the result directly.
  return priv_set_node_column_by_idx(out_name, local_cc_map);
}

}  // namespace metalldata
