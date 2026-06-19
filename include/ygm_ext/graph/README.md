// Does multiedge matter when it comes to static_graph???
      * maybe TC?
      * BFS, CC, SCC, kcore
* directed_static_graph
      * BFS, SCC, WCC
* undirected_static_graph //rh
      * BFS, CC, kcore
* dodgr_static_graph
      * TC


detail_static_graph::edge_data
detail_static_graph::node_data

async_visit_neighbors
async_visit_edges
in/out versions


notes:   multi can be std::forward_list with this specialization for monostate

template<typename T>
using item_storage =
    std::conditional_t<
        std::is_same_v<T, std::monostate>,
        std::size_t,
        std::forward_list<T>>;