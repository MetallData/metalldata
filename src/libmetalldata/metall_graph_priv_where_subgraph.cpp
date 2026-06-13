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
    priv_for_all_nodes([&to_return](local_node_idx_type nid) {
      to_return.first.push_back(nid);
    });
    priv_for_all_edges([&to_return](local_edge_idx_type eid) {
      to_return.second.push_back(eid);
    });
  } else if (where.is_node_clause()) {
    // 1. Compute the set of nodes that satisfy the node where clause.
    nodeset filtered_nodes(m_comm);
    priv_for_all_nodes_nwhere(
      [&](local_node_idx_type nid) {
        to_return.first.push_back(nid);
        auto u = priv_local_get_node_label(nid);
        YGM_ASSERT_DEBUG(u.has_value());
        auto uo = priv_local_get_node_locator(u.value());
        YGM_ASSERT_DEBUG(uo.has_value());
        filtered_nodes.async_insert(uo.value());
      },
      where);

    // 2. Gather list of nodes needed by rank local edges
    std::set<node_locator> nodes_i_need;
    priv_for_all_edges([&](local_edge_idx_type eid) {
      auto uv_o = priv_local_get_edge_uv_labels(eid);
      if (uv_o.has_value()) {
        auto [u, v] = uv_o.value();
        auto uo = priv_local_get_node_locator(u);
        auto vo = priv_local_get_node_locator(v);
        YGM_ASSERT_DEBUG(uo.has_value() && vo.has_value());
        nodes_i_need.insert(uo.value());
        nodes_i_need.insert(uo.value());
      }
    });
    std::set<node_locator> nodes_alive =
      filtered_nodes.gather_values(nodes_i_need);

    // 3. Compute the set of edges that are incident on those nodes.
    priv_for_all_edges([&](local_edge_idx_type eid) {
      auto uv_o = priv_local_get_edge_uv_labels(eid);
      if (uv_o.has_value()) {
        auto [u, v] = uv_o.value();
        auto uo = priv_local_get_node_locator(u);
        auto vo = priv_local_get_node_locator(v);
        YGM_ASSERT_DEBUG(uo.has_value() && vo.has_value());
        if (nodes_alive.contains(uo.value()) &&
            nodes_alive.contains(vo.value())) {
          to_return.second.push_back(eid);
        }
      }
    });
  } else if (where.is_edge_clause()) {
    // 1. compute the set of edges that satisfy the edge where clause & save
    // vertex labels
    nodeset nodesalive(m_comm);
    priv_for_all_edges_ewhere(
      [&](local_edge_idx_type eid) {
        auto uv_o = priv_local_get_edge_uv_labels(eid);
        YGM_ASSERT_RELEASE(uv_o.has_value());
        auto [u, v] = uv_o.value();
        auto uo = priv_local_get_node_locator(u);
        auto vo = priv_local_get_node_locator(v);
        YGM_ASSERT_DEBUG(uo.has_value() && vo.has_value());
        nodesalive.async_insert(uo.value());
        nodesalive.async_insert(vo.value());
      },
      where);

    // 2. Compute node ids from vertex labels
    nodesalive.for_all_local(
      [&](local_node_idx_type nl) { to_return.first.push_back(nl); });
  }

  else {
    YGM_ASSERT_RELEASE(false);
  }

  return to_return;
}

}  // namespace metalldata