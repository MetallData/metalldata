
#include <iterator>
#include <utility>
#include <variant>
#include <ranges>
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/array.hpp>
#include <ygm/container/bag.hpp>
#include <ygm_ext/graph/simple_edgelist.hpp>

namespace ygm_ext::graph {

template <typename NodeLabel, typename EdgeWeight = std::monostate>
class static_graph {
  using self_type = static_graph<NodeLabel, EdgeWeight>;
  using ygm_ptr_type = typename ygm::ygm_ptr<self_type>;

 public:
  enum class node_locator : size_t;

  static_graph() = delete;

  // @todo make el.comm() const
  static_graph(ygm::comm& comm,
               const ygm_ext::graph::simple_edgelist<NodeLabel, EdgeWeight>& el)
      : m_comm(comm),
        pthis(this),
        m_sorted_deg_nlb(comm, 0),
        m_nlb_to_nloc(comm),
        m_directed(el.is_directed()),
        m_multigraph(false) {
    pthis.check(m_comm);
    priv_ingest_edgelist(el);
  }

  size_t num_nodes() const { return m_sorted_deg_nlb.size(); }

  auto nodes_begin() const { return priv_node_range().begin(); }

  void nodes_end() const { return priv_node_range().end(); }

  bool is_directed() const { return m_directed; }

  bool is_multigraph() const { return m_multigraph; }

 private:
  auto priv_node_range() {
    m_nlb_to_nloc.comm().barrier();
    if (m_nlb_to_nloc.local_empty()) {
      return std::views::iota(size_t{0}, size_t{0}) |
             std::views::transform([](size_t i) { return node_locator{i}; });
    }
    size_t first = m_nlb_to_nloc.local_begin()->first;
    size_t dist =
      std::distance(m_nlb_to_nloc.local_begin(), m_nlb_to_nloc.local_end());
    return std::views::iota(first, first + dist) |
           std::views::transform([](size_t i) { return node_locator{i}; });
  }

  template <typename EL>
  void priv_ingest_edgelist(const EL& el) {
    // Count degrees of nodes in the graph.
    ygm::container::counting_set<NodeLabel> ndegree(m_comm);
    for (const auto& pack : el) {
      ndegree.async_insert(EL::source(pack));
      if (is_directed()) {
        ndegree.async_insert(EL::target(pack));
      }
    }

    // Sort nodes by degree.
    //
    // Can't directly copy into Array because Array needs .size().   Could we
    // add size to transform?
    // Also, remove comm from YGM containers and just pass it to the
    // constructor?
    {
      ygm::container::bag<std::pair<size_t, NodeLabel>> deg_nlb(
        m_nlb_to_nloc.comm(), ndegree.transform([](NodeLabel nl, size_t c) {
          return std::make_pair(c, nl);
        }));
      ndegree.clear();
      ygm::container::array<std::pair<size_t, NodeLabel>> sorted_deg_nlb(
        m_nlb_to_nloc.comm(), deg_nlb);
      m_sorted_deg_nlb = std::move(sorted_deg_nlb);
    }
    m_sorted_deg_nlb.sort();

    //
    // Create mapping from node label to node locator (index in sorted array).
    for (const auto& nid_nlb : m_sorted_deg_nlb) {
      NodeLabel nl = nid_nlb.template get<1>().second;
      size_t    idx = nid_nlb.template get<0>();
      m_nlb_to_nloc.async_insert(nl, node_locator{idx});
    }

    //
    // second pass to partition edges.   gather needed locators.   add ygm ptr.
    static self_type* spthis = nullptr;
    spthis = this;
  }
  ygm::comm&                                          m_comm;
  ygm::container::array<std::pair<size_t, NodeLabel>> m_sorted_deg_nlb;
  ygm::container::map<NodeLabel, node_locator>        m_nlb_to_nloc;
  ygm_ptr_type                                        pthis;
  bool                                                m_directed;
  bool                                                m_multigraph;
};

}  // namespace ygm_ext::graph