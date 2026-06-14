#pragma once
#include <random>
#include <unordered_set>
#include <vector>
#include <utility>

#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>

namespace metalldata {

/**
 * @brief Draws a distributed random sample of up to `k` unique elements from a
 *        globally distributed collection.
 *
 * Rank 0 selects `min(global_count, k)` unique global indices using a seeded
 * Mersenne Twister and broadcasts them to all ranks. Each rank then maps the
 * selected indices that fall within its local partition back to element values
 * using `filtered_ids` and returns those elements.
 *
 * @tparam T Element type. Must be hashable (usable as an `std::unordered_set`
 *           key) and copyable.
 *
 * @param comm         YGM communicator spanning all participating ranks.
 * @param filtered_ids Local partition of candidate elements on this rank.
 *                     Elements are treated as a contiguous segment of a
 *                     virtual global array ordered by rank.
 * @param k            Maximum number of elements to sample globally.
 * @param seed         RNG seed for reproducible sampling (used only on rank 0).
 *
 * @return An `std::unordered_set<T>` containing this rank's share of the
 *         sampled elements. Empty on ranks that own none of the selected
 *         global indices.
 *
 * @note All ranks must call this function collectively; it contains an
 *       internal barrier and collective operations (`ygm::sum`,
 *       `ygm::prefix_sum`, `ygm::bcast`).
 */
template <typename T>
std::unordered_set<T> random_sample(ygm::comm&            comm,
                                    const std::vector<T>& filtered_ids,
                                    const size_t k, uint64_t seed) {
  size_t local_count = filtered_ids.size();
  size_t global_count = ygm::sum(local_count, comm);
  size_t sample_size = std::min(global_count, k);
  size_t lower_bound = ygm::prefix_sum(local_count, comm);

  comm.barrier();

  std::vector<size_t> selected_indices;
  selected_indices.reserve(sample_size);

  if (comm.rank0()) {
    std::unordered_set<size_t>            selection;
    std::mt19937                          gen(seed);
    std::uniform_int_distribution<size_t> dist(0, global_count - 1);

    while (selection.size() < sample_size) {
      selection.insert(dist(gen));
    }

    selected_indices.assign(selection.begin(), selection.end());
  }

  ygm::bcast(selected_indices, 0, comm);

  std::unordered_set<T> local_data;

  for (const auto idx : selected_indices) {
    if ((idx >= lower_bound) && (idx < lower_bound + local_count)) {
      // local idx is guaranteed to be >= 0
      T id = filtered_ids.at(idx - lower_bound);
      YGM_ASSERT_RELEASE(!local_data.contains(id));
      local_data.insert(id);
    }
  }

  return local_data;
}
}  // namespace metalldata