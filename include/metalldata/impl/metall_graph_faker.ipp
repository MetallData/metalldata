// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <metalldata/metall_graph.hpp>
#include <type_traits>
#include <format>
#include <faker-cxx/faker.h>  // or include specific module

namespace metalldata {
template <typename Fn, typename T>
result<> metall_graph::add_faker_series(const metall_graph::series_name& name,
                                        Fn                  faker_func,
                                        const where_clause& where) {
  result<> to_return;
  auto     first = faker_func();
  using FT                        = std::decay_t<decltype(first)>;

  static_assert(std::is_constructible_v<T, FT>,
                "Invalid type for data; cannot proceed");

  if (name.is_edge_series()) {
    if (has_edge_series(name)) {
      return std::unexpected(
        std::format("edge series {} already exists", name.qualified()));
    }
    auto ser_ind = priv_add_edge_series<T>(name.unqualified());
    auto rec_p   = m_pedges;
    priv_for_all_edges(
      [&](local_edge_idx_type eid) {
        T val = T(faker_func());
        pl_set_edge_field(ser_ind, eid, val);
      },
      where);
  } else if (name.is_node_series()) {
    if (has_node_series(name)) {
      return std::unexpected(
        std::format("node series {} already exists", name.qualified()));
    }
    auto ser_ind = priv_add_node_series<T>(name.unqualified());
    auto rec_p   = m_pnodes;
    priv_for_all_nodes(
      [&](local_node_idx_type nid) {
        T val = T(faker_func());
        pl_set_node_field(ser_ind, nid, val);
      },
      where);
  }

  return to_return;
}

}  // namespace metalldata