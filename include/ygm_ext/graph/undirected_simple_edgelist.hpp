#pragma once

#include <variant>
#include <ygm/container/map.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/comm.hpp>
#include <ygm_ext/graph/detail/edge_pack.hpp>

namespace ygm_ext::graph {

template <typename NodeLabel, typename EdgeWeight = std::monostate>
class undirected_simple_edgelist {
 private:
  using storage_type =
    ygm::container::map<std::pair<NodeLabel, NodeLabel>, EdgeWeight>;

 public:
  using node_label_type = NodeLabel;
  using edge_weight_type = EdgeWeight;
  using edge_pack_type = detail::edge_pack_type<NodeLabel, EdgeWeight>;

  undirected_simple_edgelist() = delete;

  undirected_simple_edgelist(ygm::comm& comm) : m_emap(comm) {}

  // void async_insert(const node_label_type& u, const node_label_type& v,
  //                   const edge_weight_type& weight = edge_weight_type{}) {
  //   if (u == v) {
  //     return;
  //   }
  //   if (u < v) {
  //     m_emap.async_insert({u, v}, weight);
  //   } else {
  //     m_emap.async_insert({v, u}, weight);
  //   }
  // }

  void async_insert_or_assign(
    const node_label_type& u, const node_label_type& v,
    const edge_weight_type& weight = edge_weight_type{}) {
    if (u == v) {
      return;
    }
    if (u < v) {
      m_emap.async_insert_or_assign({u, v}, weight);
    } else {
      m_emap.async_insert_or_assign({v, u}, weight);
    }
  }

  // todo:   async_reduce(k, value, reduction) for things like counting edges

  ygm::container::counting_set<node_label_type> count_degrees() const {
    ygm::container::counting_set<node_label_type> to_return(m_emap.comm());
    for (const edge_pack_type& ep : m_emap) {
      to_return.async_insert(detail::source(ep));
      to_return.async_insert(detail::target(ep));
    }
    return to_return;
  }

  auto begin() { return m_emap.begin(); }

  auto begin() const { return m_emap.begin(); }

  auto end() { return m_emap.end(); }

  auto end() const { return m_emap.end(); }

  size_t size() const { return m_emap.size(); }

  void clear() { m_emap.clear(); }

  ygm::comm& comm() { return m_emap.comm(); }

 private:
  storage_type m_emap;
};

}  // namespace ygm_ext::graph