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
  std::pair<std::vector<local_node_idx_type>,
            std::vector<local_edge_idx_type>>
    to_return;

  if (where.empty()) {
    // if the where clause is empty, then we just return all nodes and edges.
  }
  if (where.is_node_clause()) {
    // 1. compute the set of nodes that satisfy the node where clause.
    // 2. compute the set of edges that are incident on those nodes.
    priv_for_all_nodes_nwhere(
      [&](local_node_idx_type nidx) { to_return.first.push_back(nidx); },
      where);
  } else if (where.is_edge_clause()) {
    // 1. compute the set of edges that satisfy the edge where clause.
    // 2. compute the set of nodes that are incident on those edges.
    priv_for_all_edges_nwhere(
      [&](local_edge_idx_type eidx) {
        to_return.second.push_back(eidx);
      },
      where);
  }

  return to_return;
}

}  // namespace metalldata