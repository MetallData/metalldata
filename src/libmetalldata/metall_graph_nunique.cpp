// Copyright 2026 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT
#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>
#include <ygm/container/counting_set.hpp>

#include <metalldata/metall_graph.hpp>
#include "multiseries/multiseries_record.hpp"
#include "ygm/container/set.hpp"

namespace metalldata {

std::map<metall_graph::series_name, size_t> metall_graph::nunique_edge(
  std::unordered_set<metall_graph::series_name> series_names,
  const where_clause                           &where) {
  std::map<metall_graph::series_name, size_t> nunique;
  std::map<metall_graph::series_name,
           std::set<multiseries::basic_record_store<>::series_type>>
    uniques;
  // get the eids that pass the where clause.
  std::vector<local_edge_idx_type> eids;
  priv_for_all_edges([&](local_edge_idx_type id) { eids.push_back(id); },
                     where);

  // map the series names to indices
  std::vector<edge_series_idx_type> sids;
  for (const auto &sname : series_names) {
    auto sid_o = pl_find_edge_series(sname);
    if (!sid_o.has_value()) {
      continue;
    }
    auto sid = sid_o.value();
    if (priv_is_edge_series_type<std::string_view>(sid)) {
      ygm::container::set<std::string> distinct(m_comm);
      for (auto eid : eids) {
        auto val_o = pl_get_edge_field<std::string_view>(sid, eid);
        if (val_o.has_value()) {
          distinct.async_insert(std::string(val_o.value()));
        }
      }

      size_t sz = distinct.size();
      if (m_comm.rank0()) {
        nunique[sname] = sz;
      }
    } else if (priv_is_edge_series_type<int64_t>(sid)) {
      ygm::container::set<int64_t> distinct(m_comm);
      for (auto eid : eids) {
        auto val_o = pl_get_edge_field<int64_t>(sid, eid);
        if (val_o.has_value()) {
          distinct.async_insert(val_o.value());
        }
      }
      size_t sz = distinct.size();
      if (m_comm.rank0()) {
        nunique[sname] = sz;
      }
    } else if (priv_is_edge_series_type<bool>(sid)) {
      bool has_true = false;
      bool has_false = false;
      // How can we share a value across ranks? A set of bool makes no sense
      // here.
      ygm::container::set<bool> distinct(m_comm);
      for (auto eid : eids) {
        auto val_o = pl_get_edge_field<bool>(sid, eid);
        if (val_o.has_value()) {
          if (val_o.value()) {
            has_true = true;
          } else {
            has_false = true;
          }
        }
        if (has_true && has_false) {
          break;
        }
      }
      bool global_has_true = ygm::logical_or(has_true, m_comm);
      bool global_has_false = ygm::logical_or(has_false, m_comm);

      if (m_comm.rank0()) {
        size_t sz = size_t(global_has_true) + size_t(global_has_false);
        nunique[sname] = sz;
      }
    } else if (priv_is_edge_series_type<double>(sid)) {
      ygm::container::set<double> distinct(m_comm);
      for (auto eid : eids) {
        auto val_o = pl_get_edge_field<double>(sid, eid);
        if (val_o.has_value()) {
          distinct.async_insert(val_o.value());
        }
      }
      size_t sz = distinct.size();
      if (m_comm.rank0()) {
        nunique[sname] = sz;
      }
    }
  }
  return nunique;
}

std::map<metall_graph::series_name, size_t> metall_graph::nunique_node(
  std::unordered_set<metall_graph::series_name> series_names,
  const where_clause                           &where) {
  std::map<metall_graph::series_name, size_t> nunique;
  std::map<metall_graph::series_name,
           std::set<multiseries::basic_record_store<>::series_type>>
    uniques;
  // get the nids that pass the where clause.
  std::vector<local_node_idx_type> nids;
  priv_for_all_nodes([&](local_node_idx_type id) { nids.push_back(id); }, where);

  // map the series names to indices
  // TODO: pl_find_node_series is overloaded to take a vector. We
  // should use that instead.
  std::vector<series_index_type> sids;
  for (const auto &sname : series_names) {
    auto sid_o = pl_find_node_series(sname);
    if (!sid_o.has_value()) {
      continue;
    }
    auto sid = sid_o.value();
    if (priv_is_node_series_type<std::string_view>(sid)) {
      ygm::container::set<std::string> distinct(m_comm);
      for (auto nid : nids) {
        auto val_o = pl_get_node_field<std::string_view>(sid, nid);
        if (val_o.has_value()) {
          distinct.async_insert(std::string(val_o.value()));
        }
      }

      size_t sz = distinct.size();
      if (m_comm.rank0()) {
        nunique[sname] = sz;
      }
    } else if (priv_is_node_series_type<int64_t>(sid)) {
      ygm::container::set<int64_t> distinct(m_comm);
      for (auto nid : nids) {
        auto val_o = pl_get_node_field<int64_t>(sid, nid);
        if (val_o.has_value()) {
          distinct.async_insert(val_o.value());
        }
      }
      size_t sz = distinct.size();
      if (m_comm.rank0()) {
        nunique[sname] = sz;
      }
    } else if (priv_is_node_series_type<bool>(sid)) {
      bool has_true = false;
      bool has_false = false;
      // How can we share a value across ranks? A set of bool makes no sense
      // here.
      ygm::container::set<bool> distinct(m_comm);
      for (auto nid : nids) {
        auto val_o = pl_get_node_field<bool>(sid, nid);
        if (val_o.has_value()) {
          if (val_o.value()) {
            has_true = true;
          } else {
            has_false = true;
          }
        }
        if (has_true && has_false) {
          break;
        }
      }
      bool global_has_true = ygm::logical_or(has_true, m_comm);
      bool global_has_false = ygm::logical_or(has_false, m_comm);

      if (m_comm.rank0()) {
        size_t sz = size_t(global_has_true) + size_t(global_has_false);
        nunique[sname] = sz;
      }
    } else if (priv_is_node_series_type<double>(sid)) {
      ygm::container::set<double> distinct(m_comm);
      for (auto nid : nids) {
        auto val_o = pl_get_node_field<double>(sid, nid);
        if (val_o.has_value()) {
          distinct.async_insert(val_o.value());
        }
      }
      size_t sz = distinct.size();
      if (m_comm.rank0()) {
        nunique[sname] = sz;
      }
    }
  }
  return nunique;
}

}  // namespace metalldata
