// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <expected>
#include <ygm/utility/boost_json.hpp>

namespace metalldata {
std::expected<bjsn::array, std::string> metall_graph::select_edges(
  const std::unordered_set<metall_graph::series_name>& series_set,
  const metall_graph::where_clause&                    where) {
  if (series_set.empty()) {
    return {};
  }

  for (const auto& s : series_set) {
    if (!s.is_edge_series()) {
      return std::unexpected("All series must be of type edge.");
    }
  }

  bjsn::array select_results_arr;
  priv_for_all_edges(
    [&](auto rid) {
      bjsn::object edge_obj;

      for (const auto& series : series_set) {
        // TODO: make this better. This is potentially expensive because we
        // have to do a field lookup on every edge.
        visit_edge_field(series, rid, [&](auto val) {
          using T = std::decay_t<decltype(val)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            edge_obj[series.unqualified()] = std::string(val);
          } else {
            edge_obj[series.unqualified()] = val;
          }
        });
      }
      select_results_arr.push_back(edge_obj);
    },
    where);

  std::vector<bjsn::array> everything(m_comm.size() - 1);  // don't need rank 0
  static auto&             s_everything = everything;
  m_comm.cf_barrier();
  if (!m_comm.rank0()) {
    m_comm.async(
      0,
      [](const bjsn::array& rank_data, int rank) {
        (s_everything)[rank - 1] = rank_data;
      },
      select_results_arr, m_comm.rank());
  }

  m_comm.barrier();

  if (m_comm.rank0()) {
    for (auto& el : everything) {
      select_results_arr.insert(select_results_arr.end(), el);
      el.clear();
    }
  }

  m_comm.barrier();
  return select_results_arr;
}

std::expected<bjsn::array, std::string> metall_graph::select_nodes(
  const std::unordered_set<metall_graph::series_name>& series_set,
  const metall_graph::where_clause&                    where) {
  if (series_set.empty()) {
    return {};
  }

  for (const auto& s : series_set) {
    if (!s.is_node_series()) {
      return std::unexpected(std::format(
        "All series must be of type node (got {}).", s.qualified()));
    }
  }

  bjsn::array select_results_arr;
  priv_for_all_nodes(
    [&](auto rid) {
      bjsn::object node_obj;

      for (const auto& series : series_set) {
        // TODO: make this better. This is potentially expensive because we
        // have to do a field lookup on every node.
        visit_node_field(series, rid, [&](auto val) {
          using T = std::decay_t<decltype(val)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            node_obj[series.unqualified()] = std::string(val);
          } else {
            node_obj[series.unqualified()] = val;
          }
        });
      }

      select_results_arr.push_back(node_obj);
    },
    where);

  std::vector<bjsn::array> everything(m_comm.size() - 1);  // don't need rank0
  static auto&             s_everything = everything;
  m_comm.cf_barrier();
  if (!m_comm.rank0()) {
    m_comm.async(
      0,
      [](const bjsn::array& rank_data, int rank) {
        (s_everything)[rank - 1] = rank_data;
      },
      select_results_arr, m_comm.rank());
  }

  m_comm.barrier();
  if (m_comm.rank0()) {
    for (auto& el : everything) {
      select_results_arr.insert(select_results_arr.end(), el);
      el.clear();
    }
  }

  m_comm.barrier();
  return select_results_arr;
}

}  // namespace metalldata