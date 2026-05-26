// Copyright 2026 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include "multiseries/multiseries_record.hpp"
#include "ygm/container/counting_set.hpp"

namespace metalldata {
ygm::container::counting_set<metall_graph::data_types>
metall_graph::value_counts(metall_graph::series_name sname,
                           const where_clause       &where) {
  ygm::container::counting_set<data_types> counts(m_comm);
  if (sname.is_edge_series()) {
    auto sid_o = m_pedges->find_series(sname.unqualified());
    if (!sid_o.has_value()) {
      return counts;
    }
    auto sid = sid_o.value();
    priv_for_all_edges(
      [&](record_id_type rid) {
        auto s_val = m_pedges->get_dynamic(sid, rid);
        // need to convert between variants. Except for string, this should be
        // zero-allocation.
        if (!s_val.has_value()) {
          return;
        }
        auto val = priv_series_to_data_type(s_val.value());

        counts.async_insert(val);
      },
      where);

  } else {
    auto sid_o = m_pnodes->find_series(sname.unqualified());
    if (!sid_o.has_value()) {
      return counts;
    }
    auto sid = sid_o.value();
    priv_for_all_nodes(
      [&](record_id_type rid) {
        auto s_val = m_pnodes->get_dynamic(sid, rid);
        // need to convert between variants. Except for string, this should be
        // zero-allocation.
        if (!s_val.has_value()) {
          return;
        }
        auto val = priv_series_to_data_type(s_val.value());

        counts.async_insert(val);
      },
      where);
  }

  return counts;
}

std::map<metall_graph::data_types, size_t> metall_graph::value_counts_topk(
  metall_graph::series_name sname, int k, const where_clause &where) {
  auto counts = value_counts(sname, where);

  std::vector<std::pair<metalldata::metall_graph::data_types, size_t>> kresults;
  if (k < 0) {
    kresults = counts.gather_topk(
      -k, [&](auto &&i, auto &&j) { return i.second < j.second; });
  } else {
    kresults = counts.gather_topk(
      k, [&](auto &&i, auto &&j) { return i.second > j.second; });
  }

  std::map<metall_graph::data_types, size_t> topk;
  for (const auto &pair : kresults) {
    topk[pair.first] = pair.second;
  }
  return topk;
}
}  // namespace metalldata