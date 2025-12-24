#pragma once
#include <metalldata/metall_graph.hpp>
#include <queue>
#include "ygm/detail/collective.hpp"
#include "ygm/utility/assert.hpp"

namespace metalldata {
// template <typename Compare = std::greater<value_type>>
// std::vector<value_type> gather_topk(size_t  k,
//                                     Compare comp =
//                                     std::greater<value_type>())
//     const requires SingleItemTuple<for_all_args> {
//   const auto*      derived_this = static_cast<const derived_type*>(this);
//   const ygm::comm& mycomm       = derived_this->comm();
//   std::vector<value_type> local_topk;

//   //
//   // Find local top_k
//   for_all([&local_topk, comp, k](const value_type& value) {
//     local_topk.push_back(value);
//     std::sort(local_topk.begin(), local_topk.end(), comp);
//     if (local_topk.size() > k) {
//       local_topk.pop_back();
//     }
//   });

//   //
//   // All reduce global top_k
//   auto to_return = ::ygm::all_reduce(
//       local_topk,
//       [comp, k](const std::vector<value_type>& va,
//                 const std::vector<value_type>& vb) {
//         std::vector<value_type> out(va.begin(), va.end());
//         out.insert(out.end(), vb.begin(), vb.end());
//         std::sort(out.begin(), out.end(), comp);
//         while (out.size() > k) {
//           out.pop_back();
//         }
//         return out;
//       },
//       mycomm);
//   return to_return;
// }

template <typename Compare>
std::vector<metall_graph::data_types> metall_graph::topk(
  size_t k, const series_name& ser_name,
  const std::vector<series_name>& ser_inc, Compare comp,
  const where_clause& where) {
  // first, get the local indices for the top K
  if (ser_name.is_node_series()) {
    if (!has_node_series(ser_name)) {
      return {};
    }

    // we make sure that the compared column is element 0. This
    // also guarantees that the vector is not empty.
    std::vector<std::string> ser_inc_unq{std::string(ser_name.unqualified())};

    for (const auto& ser : ser_inc) {
      ser_inc_unq.emplace_back(ser.unqualified());
    }

    auto ser_idxs_opt = m_pnodes->find_series(ser_inc_unq);
    auto pnodes       = m_pnodes;
    YGM_ASSERT_RELEASE(ser_idxs_opt.has_value());
    auto series_idxs = ser_idxs_opt.value();
    YGM_ASSERT_RELEASE(!series_idxs.empty());

    // Comparator for the priority queue (inverted for min-heap behavior)
    auto row_comp = [&comp](const std::vector<data_types>& a,
                            const std::vector<data_types>& b) {
      return std::visit(
        [&comp](const auto& va, const auto& vb) -> bool {
          using A = std::decay_t<decltype(va)>;
          using B = std::decay_t<decltype(vb)>;
          if constexpr (std::is_same_v<A, B>) {
            return comp(va, vb);
          }
          return false;
        },
        a.front(), b.front());
    };

    // Min-heap: keeps smallest at top, so we can pop it when size > k
    std::priority_queue<std::vector<data_types>,
                        std::vector<std::vector<data_types>>,
                        decltype(row_comp)>
      min_heap(row_comp);

    priv_for_all_nodes([&](auto rid) {
      std::vector<data_types> row = pnodes->get(ser_idxs_opt.value(), rid);
      min_heap.push(std::move(row));
      if (min_heap.size() > k) {
        min_heap.pop();  // Remove the smallest
      }
    });

    // Extract results (will be in reverse order)
    std::vector<std::vector<data_types>> topk_rows;
    topk_rows.reserve(min_heap.size());
    while (!min_heap.empty()) {
      topk_rows.push_back(min_heap.top());
      min_heap.pop();
    }
    // Reverse to get descending order
    std::reverse(topk_rows.begin(), topk_rows.end());

    YGM_ASSERT_RELEASE(topk_rows.size() <= k);
    // topk_rows is now sorted and max length k.
    // now we need to allgather.

    auto to_return = ygm::all_reduce(
      topk_rows,
      [comp, k](const std::vector<data_types>& va,
                const std::vector<data_types>& vb) {
        std::vector<data_types> out(va.begin(), va.end());
        out.insert(out.end(), vb.begin(), vb.end());

        std::sort(out.begin(), out.end(), row_comp);
        out.resize(k);
      },
      m_comm);
    return to_return;
  }
  //   auto to_return = ::ygm::all_reduce(
  //       local_topk,
  //       [comp, k](const std::vector<value_type>& va,
  //                 const std::vector<value_type>& vb) {
  //         std::vector<value_type> out(va.begin(), va.end());
  //         out.insert(out.end(), vb.begin(), vb.end());
  //         std::sort(out.begin(), out.end(), comp);
  //         while (out.size() > k) {
  //           out.pop_back();
  //         }
  //         return out;
  //       },
  //       mycomm);
  //   return to_return;
  // }
}

}  // namespace metalldata
