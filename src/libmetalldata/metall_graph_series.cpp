#include <metalldata/metall_graph.hpp>
#include "metalldata/impl/metall_graph_series_name.hpp"

namespace metalldata {

bool metall_graph::series_name::operator<(
  const metall_graph::series_name& other) const {
  if (m_prefix != other.m_prefix) {
    return m_prefix < other.m_prefix;
  }
  return m_unqualified < other.m_unqualified;
}

std::pair<std::string_view, std::string_view>
metall_graph::series_name::priv_split_series_str(std::string_view str) {
  std::string_view prefix;
  std::string_view unqualified;
  size_t           pos = str.find('.');
  if (pos != std::string_view::npos) {
    prefix = str.substr(0, pos);
    unqualified = str.substr(pos + 1);
  } else {
    prefix = std::string_view{};
    unqualified = str;
  }
  return std::make_pair(prefix, unqualified);
}

std::map<std::string, std::string> metall_graph::get_edge_selector_info() {
  // Since the m_pedges schema is identical across ranks, we don't have to
  // collect. Also: the "edge" prefix (and "node" in the corresponding
  // function) need to match the corresponding meta.json values.
  std::map<std::string, std::string> sels;
  for (const auto& el : m_pedges->get_series_names()) {
    auto sel = std::format("edge.{}", el);
    sels[sel] = "default";
  }
  for (const auto& el : m_pnodes->get_series_names()) {
    auto sel = std::format("{}.{}", series_name::U_COL.qualified(), el);
    sels[sel] = "inherited";
    sel = std::format("{}.{}", series_name::V_COL.qualified(), el);
    sels[sel] = "inherited";
  }

  return sels;
}

std::map<std::string, std::string> metall_graph::get_node_selector_info() {
  // Since the m_pedges schema is identical across ranks, we don't have to
  // collect.
  std::map<std::string, std::string> sels;
  for (const auto& el : m_pnodes->get_series_names()) {
    auto sel = std::format("node.{}", el);
    sels[sel] = "default";
  }
  return sels;
}

std::map<std::string, std::string> metall_graph::get_selector_info() {
  std::map<std::string, std::string> sels = get_edge_selector_info();

  std::map<std::string, std::string> nsels = get_node_selector_info();
  sels.insert(nsels.begin(), nsels.end());

  return sels;
}

std::pair<std::string_view, std::string_view>
metall_graph::pl_get_edge_uv_labels(
  metall_graph::local_edge_idx_type eid) const {
  auto u = pl_get_edge_field<std::string_view>(m_u_col_idx, eid);
  auto v = pl_get_edge_field<std::string_view>(m_v_col_idx, eid);
  YGM_ASSERT_DEBUG(u.has_value() && v.has_value());
  return std::make_pair(u.value(), v.value());
}

std::pair<metall_graph::node_locator, metall_graph::node_locator>
metall_graph::pl_get_edge_uv_locators(
  metall_graph::local_edge_idx_type eid) const {
  auto [ulb, vlb] = pl_get_edge_uv_labels(eid);
  auto uloc_o = pl_get_node_locator(ulb);
  auto vloc_o = pl_get_node_locator(vlb);
  YGM_ASSERT_DEBUG(uloc_o.has_value() && vloc_o.has_value());
  return std::make_pair(uloc_o.value(), vloc_o.value());
}

bool metall_graph::pl_edge_is_directed(
  metall_graph::local_edge_idx_type eid) const {
  auto odir = pl_get_edge_field<bool>(m_dir_col_idx, eid);
  YGM_ASSERT_DEBUG(odir.has_value());
  return odir.value();
}

std::string_view metall_graph::pl_get_node_label(
  metall_graph::local_node_idx_type nid) const {
  auto l = pl_get_node_field(m_node_col_idx, nid);
  YGM_ASSERT_DEBUG(l.has_value());
  YGM_ASSERT_DEBUG(std::holds_alternative<std::string_view>(l.value()));
  return std::get<std::string_view>(l.value());
}

std::optional<metall_graph::series_types> metall_graph::pl_get_node_field(
  metall_graph::node_series_idx_type sid,
  metall_graph::local_node_idx_type  nid) const {
  return m_pnodes->get_dynamic(std::to_underlying(sid),
                               std::to_underlying(nid));
}

std::optional<metall_graph::node_series_idx_type>
metall_graph::pl_find_node_series(series_name name) const {
  auto ret = m_pnodes->find_series(name.unqualified());
  if (ret.has_value()) {
    return node_series_idx_type{static_cast<node_series_idx_type>(ret.value())};
  }
  return std::nullopt;
}

std::vector<std::optional<metall_graph::node_series_idx_type>>
metall_graph::pl_find_node_series(
  const std::vector<metall_graph::series_name>& names) const {
  std::vector<std::optional<node_series_idx_type>> ret;
  ret.reserve(names.size());

  for (const auto& n : names) {
    ret.emplace_back(pl_find_node_series(n));
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
metall_graph::pl_find_edge_series(std::string_view name) const {
  auto ret = m_pedges->find_series(name);
  if (ret.has_value()) {
    return edge_series_idx_type{static_cast<edge_series_idx_type>(ret.value())};
  }
  return std::nullopt;
}

std::vector<std::optional<metall_graph::edge_series_idx_type>>
metall_graph::pl_find_edge_series(
  const std::vector<metall_graph::series_name>& names) const {
  std::vector<std::optional<edge_series_idx_type>> ret;
  ret.reserve(names.size());

  for (const auto& n : names) {
    ret.emplace_back(pl_find_edge_series(n.unqualified()));
  }

  return ret;
}

}  // namespace metalldata
