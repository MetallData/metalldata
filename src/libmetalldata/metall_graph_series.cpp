#include <metalldata/metall_graph.hpp>

namespace metalldata {

std::optional<std::pair<std::string_view, std::string_view>>
metall_graph::priv_local_get_edge_uv_labels(
  metall_graph::local_edge_idx_type eid) const {
  auto u = priv_local_get_edge_field<std::string_view>(m_u_col_idx, eid);
  auto v = priv_local_get_edge_field<std::string_view>(m_v_col_idx, eid);
  if (u.has_value() && v.has_value()) {
    return std::make_pair(u.value(), v.value());
  } else {
    return std::nullopt;
  }
}

std::optional<bool> metall_graph::priv_local_edge_is_directed(
  metall_graph::local_edge_idx_type eid) const {
  return priv_local_get_edge_field<bool>(m_dir_col_idx, eid);
}

std::optional<std::string_view> metall_graph::priv_local_get_node_label(
  metall_graph::local_node_idx_type nid) const {
  auto l = priv_local_get_node_field(m_node_col_idx, nid);
  if (l.has_value()) {
    if (std::holds_alternative<std::string_view>(l.value())) {
      return std::get<std::string_view>(l.value());
    }
  }
  return std::nullopt;
}

std::optional<metall_graph::series_types>
metall_graph::priv_local_get_node_field(
  metall_graph::node_series_idx_type sid,
  metall_graph::local_node_idx_type  nid) const {
  return m_pnodes->get_dynamic(std::to_underlying(sid),
                               std::to_underlying(nid));
}

std::vector<std::optional<metall_graph::node_series_idx_type>>
metall_graph::priv_local_find_node_series(
  std::vector<metall_graph::series_name> names) const {
  std::vector<std::optional<node_series_idx_type>> ret;
  ret.reserve(names.size());

  for (const auto& n : names) {
    ret.emplace_back(priv_local_find_node_series(n.unqualified()));
  }
  return ret;
}

bool metall_graph::has_node_series(
  const metall_graph::series_name& name) const {
  return name.is_node_series() && m_pnodes->contains_series(name.unqualified());
}

bool metall_graph::has_node_series(std::string_view unqualified_name) const {
  return m_pnodes->contains_series(unqualified_name);
};

bool metall_graph::has_edge_series(std::string_view unqualified_name) const {
  return m_pedges->contains_series(unqualified_name);
};

bool metall_graph::has_edge_series(
  const metall_graph::series_name& name) const {
  return name.is_edge_series() && m_pedges->contains_series(name.unqualified());
}

bool metall_graph::has_series(const metall_graph::series_name& name) const {
  return has_edge_series(name) || has_node_series(name);
}

std::vector<metall_graph::series_name> metall_graph::get_node_series_names()
  const {
  std::vector<series_name> sns;
  for (auto n : m_pnodes->get_series_names()) {
    sns.emplace_back(series_name("node", n));
  }
  return sns;
};

std::vector<metall_graph::series_name> metall_graph::get_edge_series_names()
  const {
  std::vector<series_name> sns;
  for (auto n : m_pedges->get_series_names()) {
    sns.emplace_back(series_name("edge", n));
  }
  return sns;
};

std::optional<metall_graph::edge_series_idx_type>
metall_graph::priv_local_find_edge_series(std::string_view name) const {
  auto ret = m_pedges->find_series(name);
  if (ret.has_value()) {
    return edge_series_idx_type{static_cast<edge_series_idx_type>(ret.value())};
  }
  return std::nullopt;
}

std::vector<std::optional<metall_graph::edge_series_idx_type>>
metall_graph::priv_local_find_edge_series(
  const std::vector<metall_graph::series_name>& names) const {
  std::vector<std::optional<edge_series_idx_type>> ret;
  ret.reserve(names.size());

  for (const auto& n : names) {
    ret.emplace_back(priv_local_find_edge_series(n.unqualified()));
  }

  return ret;
}

}  // namespace metalldata