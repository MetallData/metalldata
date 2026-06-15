#pragma once

#include <variant>
#include <ygm/container/map.hpp>
#include "ygm/comm.hpp"

namespace ygm_ext::graph {

template <typename NodeLabel, typename EdgeWeight = std::monostate>
class simple_edgelist {
 private:
  using storage_type =
    ygm::container::map<std::pair<NodeLabel, NodeLabel>, EdgeWeight>;

 public:
  using node_label_type = NodeLabel;
  using edge_weight_type = EdgeWeight;
  using edge_pack_type =
    std::pair<std::pair<node_label_type, node_label_type>, edge_weight_type>;

  simple_edgelist() = delete;

  simple_edgelist(ygm::comm& comm, bool dir = false)
      : m_emap(comm), m_directed(dir) {}

  void async_insert(const node_label_type& u, const node_label_type& v,
                    const edge_weight_type& weight = edge_weight_type{}) {
    if (u == v) {
      return;
    }
    if (m_directed) {
      m_emap.async_insert({u, v}, weight);
    } else {
      if (u < v) {
        m_emap.async_insert({u, v}, weight);
      } else {
        m_emap.async_insert({v, u}, weight);
      }
    }
  }

  void async_insert_or_assign(
    const node_label_type& u, const node_label_type& v,
    const edge_weight_type& weight = edge_weight_type{}) {
    if (u == v) {
      return;
    }
    if (m_directed) {
      m_emap.async_insert_or_assign({u, v}, weight);
    } else {
      if (u < v) {
        m_emap.async_insert_or_assign({u, v}, weight);
      } else {
        m_emap.async_insert_or_assign({v, u}, weight);
      }
    }
  }

  bool is_directed() const { return m_directed; }

  auto begin() { return m_emap.begin(); }

  auto begin() const { return m_emap.begin(); }

  auto end() { return m_emap.end(); }

  auto end() const { return m_emap.end(); }

  size_t size() const { return m_emap.size(); }

  void clear() { m_emap.clear(); }

  static node_label_type source(const edge_pack_type& pack) {
    return pack.first.first;
  }

  static node_label_type target(const edge_pack_type& pack) {
    return pack.first.second;
  }

  static edge_weight_type weight(const edge_pack_type& pack) {
    return pack.second;
  }

  ygm::comm& comm() { return m_emap.comm(); }

 private:
  storage_type m_emap;
  bool         m_directed;
};

}  // namespace ygm_ext::graph