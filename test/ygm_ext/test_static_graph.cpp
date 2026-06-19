#include "ygm_ext/graph/undirected_simple_edgelist.hpp"
#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm_ext/graph/undirected_static_graph.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  ygm_ext::graph::undirected_simple_edgelist<int> el(world);
  // el.async_insert(1, 2);
  // el.async_insert(2, 3);
  // el.async_insert(3, 4);
  // el.async_insert(4, 1);

  ygm_ext::graph::undirected_static_graph<int> g(world, el);

  return 0;
}