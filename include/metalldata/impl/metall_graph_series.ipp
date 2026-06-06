#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {

inline std::optional<std::pair<std::string_view, std::string_view>>
metall_graph::priv_local_edge_uv(metall_graph::local_edge_idx_type eid) const {
  auto u = priv_local_get_edge_field<std::string_view>(m_u_col_idx, eid);
  auto v = priv_local_get_edge_field<std::string_view>(m_v_col_idx, eid);
  if (u.has_value() && v.has_value()) {
    return std::make_pair(u.value(), v.value());
  } else {
    return std::nullopt;
  }
}

inline std::optional<bool> metall_graph::priv_local_edge_is_directed(
  metall_graph::local_edge_idx_type eid) const {
  return priv_local_get_edge_field<bool>(m_dir_col_idx, eid);
}

inline std::optional<std::string_view> metall_graph::priv_local_get_node_label(
  metall_graph::local_node_idx_type nid) const {
  auto l = priv_local_get_node_field(m_node_col_idx, nid);
  if (l.has_value()) {
    if (std::holds_alternative<std::string_view>(l.value())) {
      return std::get<std::string_view>(l.value());
    }
  }
  return std::nullopt;
}

inline std::optional<metall_graph::series_types>
metall_graph::priv_local_get_node_field(
  metall_graph::node_series_idx_type sid,
  metall_graph::local_node_idx_type  nid) const {
  return m_pnodes->get_dynamic(std::to_underlying(sid),
                               std::to_underlying(nid));
}

template <typename T>
inline std::optional<T> metall_graph::priv_local_get_node_field(
  metall_graph::node_series_idx_type sid,
  metall_graph::local_node_idx_type  nid) const {
  auto f = priv_local_get_node_field(sid, nid);
  if (f.has_value()) {
    if (std::holds_alternative<T>(f.value())) {
      return std::get<T>(f.value());
    }
  }
  return std::nullopt;
}

}  // namespace metalldata