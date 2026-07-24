// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <metalldata/detail/generic_locator.hpp>
#include <utility>

namespace metalldata {

detail::rank_type metall_graph::owner(metall_graph::node_locator nl) {
  return detail::owner(detail::generic_locator{std::to_underlying(nl)});
}

metall_graph::local_node_idx_type metall_graph::local(
  metall_graph::node_locator nl) {
  return metall_graph::local_node_idx_type{
    detail::local(detail::generic_locator{std::to_underlying(nl)})};
}

bool metall_graph::is_local(metall_graph::node_locator nl) const {
  return owner(nl) == m_comm.rank();
}

metall_graph::node_locator metall_graph::make_node_locator(

  detail::rank_type owner, metall_graph::local_node_idx_type nid) {
  auto gl = detail::make_generic_locator(owner, std::to_underlying(nid));
  return metall_graph::node_locator{std::to_underlying(gl)};
}

detail::rank_type metall_graph::owner(metall_graph::edge_locator el) {
  return detail::owner(detail::generic_locator{std::to_underlying(el)});
}

metall_graph::local_edge_idx_type metall_graph::local(
  metall_graph::edge_locator el) {
  return metall_graph::local_edge_idx_type{
    detail::local(detail::generic_locator{std::to_underlying(el)})};
}

bool metall_graph::is_local(metall_graph::edge_locator el) const {
  return owner(el) == m_comm.rank();
}

metall_graph::edge_locator metall_graph::make_edge_locator(
  detail::rank_type owner, metall_graph::local_edge_idx_type eid) {
  auto gl = detail::make_generic_locator(owner, std::to_underlying(eid));
  return metall_graph::edge_locator{std::to_underlying(gl)};
}

}  // namespace metalldata