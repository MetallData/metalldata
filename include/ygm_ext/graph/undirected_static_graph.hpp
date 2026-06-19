
#include <iterator>
#include <utility>
#include <variant>
#include <ranges>
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/array.hpp>
#include <ygm/container/bag.hpp>
#include <ygm_ext/graph/undirected_simple_edgelist.hpp>
#include <ygm_ext/graph/undirected_multi_edgelist.hpp>

namespace ygm_ext::graph {

template <typename NodeLabel, typename EdgeWeight = std::monostate>
class undirected_static_graph {
  using self_type = undirected_static_graph<NodeLabel, EdgeWeight>;
  using ygm_ptr_type = typename ygm::ygm_ptr<self_type>;

 public:
  enum class node_locator : size_t;

  undirected_static_graph() = delete;

  undirected_static_graph(
    ygm::comm&                                               comm,
    const undirected_simple_edgelist<NodeLabel, EdgeWeight>& el)
      : m_comm(comm), pthis(this), m_multigraph(false) {
    pthis.check(m_comm);
    priv_ingest_edgelist(el);
  }

  undirected_static_graph(
    ygm::comm& comm, const undirected_multi_edgelist<NodeLabel, EdgeWeight>& el)
      : m_comm(comm), pthis(this), m_multigraph(true) {
    pthis.check(m_comm);
    priv_ingest_edgelist(el);
  }

  // size_t num_nodes() const { return m_sorted_deg_nlb.size(); }

  // auto nodes_begin() const { return priv_node_range().begin(); }

  // void nodes_end() const { return priv_node_range().end(); }

  bool is_multigraph() const { return m_multigraph; }

 private:
  // auto priv_node_range() {
  //   m_nlb_to_nloc.comm().barrier();
  //   if (m_nlb_to_nloc.local_empty()) {
  //     return std::views::iota(size_t{0}, size_t{0}) |
  //            std::views::transform([](size_t i) { return node_locator{i}; });
  //   }
  //   size_t first = m_nlb_to_nloc.local_begin()->first;
  //   size_t dist =
  //     std::distance(m_nlb_to_nloc.local_begin(), m_nlb_to_nloc.local_end());
  //   return std::views::iota(first, first + dist) |
  //          std::views::transform([](size_t i) { return node_locator{i}; });
  // }

  template <typename EL>
  void priv_ingest_edgelist(const EL& el) {
    auto degree_counts = el.count_degrees();

    // // todo, upgrade to ranges & skip bag
    ygm::container::bag<std::pair<size_t, NodeLabel>> bag_deg_node(m_comm);
    for (const auto& dc : degree_counts) {
      bag_deg_node.async_insert({dc.second, dc.first});
    }

    // auto transformed = degree_counts | std::views::transform([](const auto& p) {
    //    return std::make_pair(p.second, p.first);});



    // ygm::container::array<std::pair<size_t, NodeLabel>> myarray(
    //   m_comm, transformed);

    //
  }
  ygm::comm&   m_comm;
  ygm_ptr_type pthis;
  bool         m_multigraph;
};

}  // namespace ygm_ext::graph


/*

ygm::graph::undirected_static_graph<metall_graph::node_locator> g;

auto bfs_level = g.make_vertex_metadata<size_t>;
auto parent = g.make_vertex_metadata<metall_graph::node_locator>;

bfs(g, bfs_level, parent);





*/