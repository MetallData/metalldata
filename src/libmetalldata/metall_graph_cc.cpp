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
  ygm::container::map<std::string,
                      std::pair<std::string, std::vector<std::string>>>
    adj_list(m_comm);

  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      auto [u, v] = pl_get_edge_uv_labels(eid);
      bool is_directed = pl_edge_is_directed(eid);
      auto adj_inserter =
        [](const std::string&                                ccid,
           std::pair<std::string, std::vector<std::string>>& adj,
           const std::string&                                vert) {
          adj.second.push_back(vert);
          adj.first = ccid;
        };
      adj_list.async_visit(std::string(u), adj_inserter, std::string(v));
      adj_list.async_visit(std::string(v), adj_inserter, std::string(u));
    },
    where);
  if (where.is_node_clause()) {
    priv_for_all_nodes_nwhere(
      [&](local_node_idx_type nid) {
        // Do something with each node
        auto nlb = pl_get_node_label(nid);
        adj_list.async_visit(
          std::string(nlb),
          [](const std::string&                                ccid,
             std::pair<std::string, std::vector<std::string>>& adj) {
            adj.first = ccid;
          });
      },
      where);
  }

  static auto* sp_adj_list = &adj_list;
  m_comm.barrier();

  struct cc_visitor {
    void operator()(const std::string&                                v,
                    std::pair<std::string, std::vector<std::string>>& adj,
                    const std::string&                                cc_id) {
      if (cc_id < adj.first) {
        adj.first = cc_id;
        for (const auto& n : adj.second) {
          sp_adj_list->async_visit(n, cc_visitor{}, cc_id);
        }
      }
    }
  };

  adj_list.for_all([&](const std::string&                                v,
                       std::pair<std::string, std::vector<std::string>>& adj) {
    auto min_id = v;
    for (const auto& n : adj.second) {
      min_id = std::min(min_id, n);
    }

    if (min_id == v) {
      for (const auto& n : adj.second) {
        sp_adj_list->async_visit(n, cc_visitor{}, v);
      }
    }
  });
  m_comm.barrier();
  // todo:  fix issue that requires string_view here
  std::map<std::string, std::string_view> local_cc_map;

  adj_list.for_all([&](const std::string&                                v,
                       std::pair<std::string, std::vector<std::string>>& adj) {
    adj.second.clear();
    adj.second.shrink_to_fit();
    local_cc_map[v] = adj.first;
  });
  // no warnings possible here, so just return the result directly.
  return set_node_column(out_name, local_cc_map);
}

}  // namespace metalldata
