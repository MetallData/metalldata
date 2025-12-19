#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
;
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
  series_name nodecol_name, const T& collection) {
  return_code to_return;

  using record_id_type = record_store_type::record_id_type;
  using val_type       = typename T::mapped_type;

  // create a node_local map of record id to node value.
  std::map<std::string, record_id_type> node_to_id{};
  m_pnodes->for_all_rows([&](record_id_type id) {
    std::string_view node =
      m_pnodes->get<std::string_view>(NODE_COL.unqualified(), id);
    node_to_id[std::string(node)] = id;
  });

  // create series and store index so we don't have to keep looking it up.
  auto nodecol_idx = m_pnodes->add_series<val_type>(nodecol_name.unqualified());

  size_t invalid_nodes = 0;
  for (const auto& [k, v] : collection) {
    if (!node_to_id.contains(k)) {
      ++invalid_nodes;
      continue;
    }
    auto node_idx = node_to_id.at(k);
    m_pnodes->set(nodecol_idx, node_idx, v);
  }

  if (invalid_nodes > 0) {
    to_return.warnings["invalid nodes"] = invalid_nodes;
  }

  return to_return;
}

}  // namespace metalldata