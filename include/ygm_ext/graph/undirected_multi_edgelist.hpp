#pragma once

#include <variant>
#include <ygm/container/bag.hpp>
#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm_ext/graph/detail/edge_pack.hpp>
#include <forward_list>

namespace ygm_ext::graph {

template <typename NodeLabel, typename EdgeWeight = std::monostate>
class undirected_multi_edgelist {
 public:
  using node_label_type = NodeLabel;
  using edge_weight_type = EdgeWeight;
  using edge_pack_type = detail::edge_pack_type<NodeLabel, EdgeWeight>;

 private:
  using storage_type = ygm::container::bag<edge_pack_type>;

 public:
  undirected_multi_edgelist() = delete;

  undirected_multi_edgelist(ygm::comm& comm) : m_ebag(comm) {}

  void async_insert(const node_label_type& u, const node_label_type& v,
                    const edge_weight_type& weight = edge_weight_type{}) {
    if (u < v) {
      m_ebag.async_visit(std::make_pair(std::make_pair(u, v), weight));
    } else {
      m_ebag.async_visit(std::make_pair(std::make_pair(v, u), weight));
    }
  }

  ygm::container::counting_set<node_label_type> count_degrees() const {
    ygm::container::counting_set<node_label_type> to_return(m_ebag.comm());
    for (const edge_pack_type& ep : m_ebag) {
      to_return.async_insert(source(ep));
      to_return.async_insert(target(ep));
    }
    return to_return;
  }

  auto begin() { return m_ebag.begin(); }

  auto begin() const { return m_ebag.begin(); }

  auto end() { return m_ebag.end(); }

  auto end() const { return m_ebag.end(); }

  size_t size() const { return m_ebag.size(); }

  void clear() { m_ebag.clear(); }

  ygm::comm& comm() { return m_ebag.comm(); }

 private:
  storage_type m_ebag;
};

}  // namespace ygm_ext::graph