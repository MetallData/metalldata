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

metall_graph::return_code metall_graph::connected_components(
  const series_name& out_name, const where_clause& where) {
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

  // TODO: convert to (rank, node row id) tuples.
  ygm::container::map<std::string,
                      std::pair<std::string, std::vector<std::string>>>
    adj_list(m_comm);

  priv_for_all_edges(
    [&](record_id_type id) {
      std::string u(m_pedges->get<std::string_view>(m_u_col_idx, id));
      std::string v(m_pedges->get<std::string_view>(m_v_col_idx, id));
      auto        is_directed = m_pedges->get<bool>(m_dir_col_idx, id);
      auto        adj_inserter =
        [](const std::string&                                ccid,
           std::pair<std::string, std::vector<std::string>>& adj,
           const std::string&                                vert) {
          adj.second.push_back(vert);
          adj.first = ccid;
        };
      adj_list.async_visit(u, adj_inserter, v);
      if (!is_directed) {
        adj_list.async_visit(v, adj_inserter, u);
      }
    },
    where);

  if (where.is_node_clause()) {
    priv_for_all_nodes_nwhere(
      [&](record_id_type id) {
        // Do something with each node
        std::string v(m_pnodes->get<std::string_view>(m_node_col_idx, id));
        adj_list.async_visit(
          v, [](const std::string&                                ccid,
                std::pair<std::string, std::vector<std::string>>& adj) {
            adj.first = ccid;
          });
      },
      where);
  }

  adj_list.for_all([&](const std::string&                                v,
                       std::pair<std::string, std::vector<std::string>>& adj) {
    adj.first = v;
    for (const auto& n : adj.second) {
      adj.first = std::min(adj.first, n);
    }
  });

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
    if (adj.first == v) {
      for (const auto& n : adj.second) {
        sp_adj_list->async_visit(n, cc_visitor{}, adj.first);
      }
    }
  });

  // todo:  fix issue that requires string_view here
  std::map<std::string, std::string_view> local_cc_map;

  adj_list.for_all([&](const std::string&                                v,
                       std::pair<std::string, std::vector<std::string>>& adj) {
    adj.second.clear();
    adj.second.shrink_to_fit();
    local_cc_map[v] = adj.first;
  });

  to_return = set_node_column(out_name, local_cc_map);

  return to_return;
}

}  // namespace metalldata