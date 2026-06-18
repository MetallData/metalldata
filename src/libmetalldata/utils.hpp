#pragma once
#include <metalldata/metall_graph.hpp>
#include "ygm/detail/collective.hpp"

namespace metalldata {
template <typename C>
C down_select(const C &collection, size_t k) {
  C smaller(collection.comm());

  size_t local_count = collection.local_size();
  size_t psum = ygm::prefix_sum(local_count, collection.comm());
  // size_t global_count = ygm::sum(local_count, collection.comm());

  collection.comm().barrier();

  size_t local_k =
    size_t(std::clamp((int(k) - int(psum)), 0, int(local_count)));

  // local_k now holds the number of elements we need to send

  size_t i = 0;
  for (auto it = collection.local_cbegin();
       it != collection.local_cend() && i < local_k; ++it, ++i) {
    smaller.local_insert(*it);
  }

  return smaller;
}
}  // namespace metalldata