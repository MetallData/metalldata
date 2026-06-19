#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm_ext/graph/directed_simple_edgelist.hpp>
#include <ygm_ext/graph/undirected_simple_edgelist.hpp>
#include <ygm_ext/graph/directed_multi_edgelist.hpp>
#include <ygm_ext/graph/undirected_multi_edgelist.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  ygm_ext::graph::undirected_simple_edgelist<int> el(world);
  el.async_insert_or_assign(1, 2);
  el.async_insert_or_assign(2, 3);
  el.async_insert_or_assign(3, 4);
  el.async_insert_or_assign(4, 1);
  YGM_ASSERT_RELEASE(el.size() == 4);
  return 0;
}