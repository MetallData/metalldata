#pragma once
#include <metalldata/metall_graph.hpp>
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

template <typename T, typename Compare>
std::vector<metall_graph::data_types> metall_graph::topk(
  size_t k, const series_name& ser_name,
  const std::vector<series_name>& ser_inc, Compare comp,
  const where_clause& where) {
  // first, get the local indices for the top K
  if (ser_name.is_node_series()) {
    if (!has_node_series(ser_name)) {
      return {};
    }

    std::vector<std::string> ser_inc_unq{std::string(ser_name.unqualified())};

    for (const auto& ser : ser_inc) {
      ser_inc_unq.emplace_back(ser.unqualified());
    }

    auto ser_idxs_opt = m_pnodes->find_series(ser_inc_unq);

    std::vector<T> ser_vals;
    auto           pnodes = m_pnodes;
    YGM_ASSERT_RELEASE(ser_idxs_opt.has_value());
    std::vector<std::pair<metall_graph::data_types,
                          std::vector<metall_graph::data_types>>>
      row_pairs;

    priv_for_all_nodes([&](auto rid) {
      std::vector<metall_graph::data_types> row =
        pnodes->get(ser_idxs_opt.value(), rid);
      data_types key_val = row.front();

      auto tup = std::make_pair(key_val, row);
      row_pairs.push_back(tup);
    });
    std::sort(
      row_pairs.begin(), row_pairs.end(),
      [&comp](const auto& a, const auto& b) { return comp(a.first, b.first); });
    row_pairs.resize(k);

    // TODO: implement
  }
  // TODO: complete implementation
  return {};
}

}  // namespace metalldata
//
//
//  namespace metalldata