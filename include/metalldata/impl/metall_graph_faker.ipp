#pragma once
#include <metalldata/metall_graph.hpp>
#include <type_traits>
#include <faker-cxx/faker.h>  // or include specific module

namespace metalldata {
template <typename Fn, typename T>
metall_graph::return_code metall_graph::add_faker_series(
  const metall_graph::series_name& name, Fn faker_func,
  const where_clause& where) {
  metall_graph::return_code to_return;
  auto                      first = faker_func();
  using FT                        = std::decay_t<decltype(first)>;

  static_assert(std::is_constructible_v<T, FT>,
                "Invalid type for data; cannot proceed");

  if (name.is_edge_series()) {
    if (has_edge_series(name)) {
      to_return.error =
        std::format("Edge series {} already exists", name.qualified());
      return to_return;
    }
    auto ser_ind = m_pedges->add_series<T>(name.unqualified());
    auto rec_p   = m_pedges;
    priv_for_all_edges(
      [&](auto rid) {
        T val = T(faker_func());
        rec_p->set(ser_ind, rid, val);
      },
      where);
  } else if (name.is_node_series()) {
    if (has_node_series(name)) {
      to_return.error =
        std::format("Node series {} already exists", name.qualified());
      return to_return;
    }
    auto ser_ind = m_pnodes->add_series<T>(name.unqualified());
    auto rec_p   = m_pnodes;
    priv_for_all_nodes(
      [&](auto rid) {
        T val = T(faker_func());
        rec_p->set(ser_ind, rid, val);
      },
      where);
  }
  return to_return;
}

}  // namespace metalldata