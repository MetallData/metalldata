#include <metalldata/metall_graph.hpp>
#include <ygm/utility/assert.hpp>
#include <vector>
#include <utility>

namespace metalldata {

std::pair<std::vector<metall_graph::local_node_idx_type>,
          std::vector<metall_graph::local_edge_idx_type>>
metall_graph::priv_where_subgraph(
  const metall_graph::where_clause& where) const {
  // first is node record ids, second is edge record ids.
  std::pair<std::vector<local_node_idx_type>, std::vector<local_edge_idx_type>>
    to_return;

  if (where.empty()) {
    // if the where clause is empty, then we just return all nodes and edges.
    priv_for_all_nodes_empty(
      [&to_return](local_node_idx_type nidx) {
        to_return.first.push_back(nidx);
      },
      where);
    priv_for_all_edges_empty(
      [&to_return](local_edge_idx_type eidx) {
        to_return.second.push_back(eidx);
      },
      where);
  } else if (where.is_node_clause()) {
    // 1. Compute the set of nodes that satisfy the node where clause.
    ygm::container::set<std::string> nodeset(m_comm);
    priv_for_all_nodes_nwhere(
      [&](local_node_idx_type nidx) {
        to_return.first.push_back(nidx);
        auto u = priv_local_get_node_label(nidx);
        if (u.has_value()) {
          nodeset.async_insert(std::string(u.value()));
        }
      },
      where);

    // 2. Gather list of nodes needed by rank local edges
    std::set<std::string> nodes_i_need;
    priv_for_all_edges_empty([&](local_edge_idx_type eidx) {
      auto ouv = priv_local_edge_uv(eidx);
      if (ouv.has_value()) {
        auto [u, v] = ouv.value();
        nodes_i_need.insert(std::string(u));
        nodes_i_need.insert(std::string(v));
      }
    });
    std::set<std::string> nodes_alive = nodeset.gather_values(nodes_i_need);

    // 3. Compute the set of edges that are incident on those nodes.
    priv_for_all_edges_empty([&](local_edge_idx_type eidx) {
      auto ouv = priv_local_edge_uv(eidx);
      if (ouv.has_value()) {
        auto [u, v] = ouv.value();
        if (nodes_alive.contains(std::string(u)) &&
            nodes_alive.contains(std::string(v))) {
          to_return.second.push_back(eidx);
        }
      }
    });
  } else if (where.is_edge_clause()) {
    // 1. compute the set of edges that satisfy the edge where clause & save
    // vertex labels
    ygm::container::set<std::string> nodeset(m_comm);
    priv_for_all_edges_ewhere(
      [&](local_edge_idx_type eidx) {
        auto ouv = priv_local_edge_uv(eidx);
        YGM_ASSERT_RELEASE(ouv.has_value());
        auto [u, v] = ouv.value();
        nodeset.async_insert(std::string(u));
        nodeset.async_insert(std::string(v));
        to_return.second.push_back(eidx);
      },
      where);

    // 2. Compute node ids from vertex labels
    for (const auto& node : nodeset) {
      auto opsa = priv_local_node_find(node);
      YGM_ASSERT_RELEASE(opsa.has_value());
      to_return.first.push_back(opsa.value());
    }
  } else {
    YGM_ASSERT_RELEASE(false);
  }

  return to_return;
}

}  // namespace metalldata