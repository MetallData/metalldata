#pragma once

#include <utility>

namespace ygm_ext::graph::detail {

template <typename NodeLabel, typename EdgeWeight>
using edge_pack_type = std::pair<std::pair<NodeLabel, NodeLabel>, EdgeWeight>;

template <typename NodeLabel, typename EdgeWeight>
inline NodeLabel source(const edge_pack_type<NodeLabel, EdgeWeight>& pack) {
  return pack.first.first;
}

template <typename NodeLabel, typename EdgeWeight>
inline NodeLabel target(const edge_pack_type<NodeLabel, EdgeWeight>& pack) {
  return pack.first.second;
}

template <typename NodeLabel, typename EdgeWeight>
inline EdgeWeight weight(const edge_pack_type<NodeLabel, EdgeWeight>& pack) {
  return pack.second;
}

}  // namespace ygm_ext::graph::detail