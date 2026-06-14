#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {

template <typename T>
std::optional<T> metall_graph::pl_get_node_field(
  metall_graph::node_series_idx_type sid,
  metall_graph::local_node_idx_type  nid) const {
  auto f = pl_get_node_field(sid, nid);
  if (f.has_value()) {
    if (std::holds_alternative<T>(f.value())) {
      return std::get<T>(f.value());
    }
  }
  return std::nullopt;
}

template <typename T>
bool metall_graph::add_series(
  const metall_graph::series_name& name) {  // "node.color" or "edge.time"
  if (has_series(name)) {
    return false;
  }
  if (name.is_node_series()) {
    m_pnodes->add_series<T>(name.unqualified());
    return true;
  }
  if (name.is_edge_series()) {
    m_pedges->add_series<T>(name.unqualified());
    return true;
  }
  return false;
}

// TODO:  Remove this, used by select...
template <typename Fn>
void metall_graph::visit_node_field(const metall_graph::series_name& name,
                                    size_t record_id, Fn func) const {
  assert(name.is_node_series());
  m_pnodes->visit_field(name.unqualified(), record_id, func);
}

// TODO:  Remove this, used by select...
template <typename Fn>
void metall_graph::visit_edge_field(const metall_graph::series_name& name,
                                    size_t record_id, Fn func) const {
  assert(name.is_edge_series());
  m_pedges->visit_field(name.unqualified(), record_id, func);
}

}  // namespace metalldata