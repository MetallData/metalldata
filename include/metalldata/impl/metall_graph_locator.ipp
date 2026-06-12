#pragma once
#include <metalldata/metall_graph.hpp>
#include <metalldata/detail/generic_locator.hpp>
#include <optional>
#include <utility>

namespace metalldata {

inline int owner(metall_graph::node_locator nl) {
  return owner(detail::generic_locator{std::to_underlying(nl)});
}

inline metall_graph::local_node_idx_type local(metall_graph::node_locator nl) {
  return metall_graph::local_node_idx_type{
    local(detail::generic_locator{std::to_underlying(nl)})};
}

inline std::optional<metall_graph::node_locator> init_node_locator(
  int owner, metall_graph::local_node_idx_type nid) {
  auto gl_o = detail::init_generic_locator(owner, std::to_underlying(nid));
  if (gl_o.has_value()) {
    return metall_graph::node_locator{std::to_underlying(gl_o.value())};
  }
  return std::nullopt;
}

inline int owner(metall_graph::edge_locator el) {
  return owner(detail::generic_locator{std::to_underlying(el)});
}

inline metall_graph::local_edge_idx_type local(metall_graph::edge_locator el) {
  return metall_graph::local_edge_idx_type{
    local(detail::generic_locator{std::to_underlying(el)})};
}

inline std::optional<metall_graph::edge_locator> init_edge_locator(
  int owner, metall_graph::local_edge_idx_type eid) {
  auto gl_o = detail::init_generic_locator(owner, std::to_underlying(eid));
  if (gl_o.has_value()) {
    return metall_graph::edge_locator{std::to_underlying(gl_o.value())};
  }
  return std::nullopt;
}

}  // namespace metalldata