// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <cctype>
#include <format>
#include <string>
#include <string_view>
#include <vector>

namespace {
// Subgraph names become clippy selectors, so they must be identifiers that do
// not start with an underscore.
bool is_valid_subgraph_name(std::string_view name) {
  if (name.empty() || !std::isalpha(static_cast<unsigned char>(name.front()))) {
    return false;
  }
  for (char c : name) {
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') {
      return false;
    }
  }
  return true;
}
}  // namespace

namespace metalldata {

result<> metall_graph::priv_check_not_hidden(const series_name& name) {
  if ((name.is_node_series() || name.is_edge_series()) && name.is_hidden()) {
    return std::unexpected(std::format(
      "series names starting with '_' are reserved: {}", name.qualified()));
  }
  return {};
}

result<> metall_graph::priv_check_where(const where_clause& where) const {
  if (where.empty() || where.is_node_clause() || where.is_edge_clause()) {
    return {};
  }
  if (!where.is_subgraph_clause()) {
    return std::unexpected(
      "invalid where clause: series must all be node, all be edge, or all be "
      "subgraph series");
  }
  for (const auto& name : where.series_names()) {
    if (!has_series(name)) {
      return std::unexpected(
        std::format("series {} not found", name.qualified()));
    }
  }
  return {};
}

result<> metall_graph::create_subgraph(std::string_view    name,
                                       const where_clause& where) {
  if (!is_valid_subgraph_name(name)) {
    return std::unexpected(std::format("invalid subgraph name: {}", name));
  }

  series_name sg_name("subgraph", name);
  auto        node_name = sg_name.subgraph_node_series();
  auto        edge_name = sg_name.subgraph_edge_series();
  if (has_series(node_name) || has_series(edge_name)) {
    return std::unexpected(
      std::format("series {} already exists", sg_name.qualified()));
  }

  if (auto chk = priv_check_where(where); !chk) {
    return chk;
  }

  const auto [subnode, subedge] = priv_where_subgraph(where);

  auto node_idx = priv_add_node_series<bool>(node_name.unqualified());
  auto edge_idx = priv_add_edge_series<bool>(edge_name.unqualified());

  for (const auto& nid : subnode) {
    pl_set_node_field(node_idx, nid, true);
  }
  for (const auto& eid : subedge) {
    pl_set_edge_field(edge_idx, eid, true);
  }

  return {};
}

std::vector<metall_graph::series_name> metall_graph::get_subgraph_names()
  const {
  // Since the schema is identical across ranks, we don't have to collect.
  std::vector<series_name> sns;
  for (std::string_view n : m_pnodes->get_series_names()) {
    if (!n.starts_with(series_name::SUBGRAPH_PREFIX)) {
      continue;
    }
    if (m_pnodes->is_series_type<bool>(n) &&
        m_pedges->is_series_type<bool>(n)) {
      n.remove_prefix(series_name::SUBGRAPH_PREFIX.size());
      sns.emplace_back(series_name("subgraph", n));
    }
  }
  return sns;
}

}  // namespace metalldata
