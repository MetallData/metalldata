#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
template <typename C>
C down_select(const C &collection, size_t k) {
  C smaller(collection.comm());

  size_t local_count = collection.local_size();
  size_t global_count = ygm::sum(local_count, collection.comm());

  size_t local_k = (local_count * k + global_count - 1) / global_count;

  // local_k now holds the ceil of number of elements needed at this rank

  size_t i = 0;
  for (auto it = collection.local_cbegin();
       it != collection.local_cend() && i < local_k; ++it, ++i) {
    smaller.local_insert(*it);
  }

  return smaller;
}
}  // namespace metalldata