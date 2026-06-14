#pragma once
#include <metalldata/metall_graph.hpp>
#include <type_traits>
#include <utility>

namespace metalldata {
template <typename T>
// This function takes a series name and a map
// of record_id_type to value and:
// 1. Creates the series
// 2. For each record id, sets the series value at that record id to the value.
//

result<> metall_graph::priv_set_edge_column_by_idx(
  const metall_graph::series_name& col_name, const T& collection) {
  using val_type = typename T::mapped_type;

  result<> to_return;
  // create series
  auto ser_idx = priv_add_edge_series<val_type>(col_name.unqualified());

  for (const auto& [eid, value] : collection) {
    pl_set_edge_field(ser_idx, eid, value);
  }

  return to_return;
}

template <typename T>
result<> metall_graph::priv_set_node_column_by_idx(
  const metall_graph::series_name& col_name, const T& collection) {
  using val_type = typename T::mapped_type;

  result<> to_return;
  // create series
  auto ser_idx = priv_add_node_series<val_type>(col_name.unqualified());

  for (const auto& [nid, value] : collection) {
    pl_set_node_field(ser_idx, nid, value);
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
metalldata::result<> metall_graph::set_node_column(
  const series_name& nodecol_name, const T& collection) {
  result<> to_return;

  // using record_id_type = record_store_type::record_id_type;
  using val_type = typename T::mapped_type;

  // create series
  node_series_idx_type nodecol_idx;
  size_t invalid_nodes = 0;
  if constexpr (std::unsigned_integral<val_type>) {
    nodecol_idx = priv_add_node_series<int64_t>(nodecol_name.unqualified());

    for (const auto& [node_name, value] : collection) {
      auto nid_o = pl_get_node_id(node_name);
      if (!nid_o.has_value()) {
        ++invalid_nodes;
        continue;
      }
      if (value > std::numeric_limits<int64_t>::max()) {
        return std::unexpected("Cannot process unsigned integer value > 2**63");
      }
      pl_set_node_field(nodecol_idx, nid_o.value(),
                        static_cast<int64_t>(value));
    }

  } else {
    nodecol_idx = priv_add_node_series<val_type>(nodecol_name.unqualified());
    for (const auto& [node_name, value] : collection) {
      auto nid_o = pl_get_node_id(node_name);
      if (!nid_o.has_value()) {
        ++invalid_nodes;
        continue;
      }
      pl_set_node_field(nodecol_idx, nid_o.value(), value);
    }
  }
  if (invalid_nodes > 0) {
    to_return.add_warnings(invalid_nodes, "invalid nodes");
  }

  return to_return;
}

}  // namespace metalldata