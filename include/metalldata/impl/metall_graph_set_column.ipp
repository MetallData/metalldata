#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
template <typename T>
// This function takes a series name and a map
// of record_id_type to value and:
// 1. Creates the series
// 2. For each record id, sets the series value at that record id to the value.
metall_graph::return_code metall_graph::priv_set_column_by_idx(
  const metall_graph::series_name& col_name, const T& collection) {
  metall_graph::return_code to_return;

  using record_id_type = metall_graph::record_store_type::record_id_type;
  using val_type       = typename T::mapped_type;

  auto store = col_name.is_edge_series() ? m_pedges : m_pnodes;
  // create series
  auto col_idx = store->add_series<val_type>(col_name.unqualified());

  size_t invalid_nodes = 0;
  for (const auto& [rid, value] : collection) {
    store->set(col_idx, rid, value);
  }

  return to_return;
}

// Sets a node metadata column based on a lookup from an associative data
// structure.
// Node names are extracted from the key.

// collection is some sort of associative data structure that has
// key->value mappings. the key is the node name. Keys that do not
// correspond to nodes are ignored, but if the number is greater than zero,
// a warning message will be added to the return code.
//
// TODO: memoize / persist the node_to_id map so that we're not building it
// every time.
template <typename T>
metall_graph::return_code metall_graph::set_node_column(
  const series_name& nodecol_name, const T& collection) {
  return_code to_return;

  using record_id_type = record_store_type::record_id_type;
  using val_type       = typename T::mapped_type;

  // create series
  auto nodecol_idx = m_pnodes->add_series<val_type>(nodecol_name.unqualified());

  size_t invalid_nodes = 0;
  for (const auto& [node_name, value] : collection) {
    auto opsv = priv_local_node_find(node_name);
    if (!opsv.has_value()) {
      ++invalid_nodes;
      continue;
    }
    m_pnodes->set(nodecol_idx, opsv.value(), value);
  }

  if (invalid_nodes > 0) {
    to_return.warnings["invalid nodes"] = invalid_nodes;
  }

  return to_return;
}

}  // namespace metalldata