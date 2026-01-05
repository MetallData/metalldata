#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
// This collection is a (local) record id to value.
template <typename T>
metall_graph::return_code metall_graph::priv_set_edge_column_by_idx(
  series_name edgecol_name, const T& collection) {
  return_code to_return;

  using record_id_type = record_store_type::record_id_type;
  using val_type       = typename T::mapped_type;

  // create series
  auto edgecol_idx = m_pedges->add_series<val_type>(edgecol_name.unqualified());

  for (const auto& [rid, value] : collection) {
    m_pedges->set(edgecol_idx, rid, value);
  }

  return to_return;
}

}  // namespace metalldata
