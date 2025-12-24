#pragma once
#include <metalldata/metall_graph.hpp>
#include <functional>
#include <queue>
#include "ygm/detail/collective.hpp"
#include "ygm/utility/assert.hpp"

namespace metalldata {

template <typename Compare>
std::vector<std::vector<metall_graph::data_types>> metall_graph::topk(
  size_t k, const series_name& ser_name,
  const std::vector<series_name>& ser_inc, Compare comp,
  const where_clause& where) {
  record_store_type* pdata;
  std::function<void(std::function<void(size_t)>, const where_clause&)>
    for_all_func;
  if (ser_name.is_edge_series()) {
    pdata        = m_pedges;
    for_all_func = [this](std::function<void(size_t)> func,
                          const where_clause&         where) {
      priv_for_all_edges(func, where);
    };
  } else if (ser_name.is_node_series()) {
    pdata        = m_pnodes;
    for_all_func = [this](std::function<void(size_t)> func,
                          const where_clause&         where) {
      priv_for_all_nodes(func, where);
    };
  } else {
    return {};
  }

  if (!has_series(ser_name)) {
    return {};
  }

  // we make sure that the compared column is element 0. This
  // also guarantees that the vector is not empty.
  std::vector<std::string> ser_inc_unq{std::string(ser_name.unqualified())};

  for (const auto& ser : ser_inc) {
    ser_inc_unq.emplace_back(ser.unqualified());
  }

  auto ser_idxs_opt = pdata->find_series(ser_inc_unq);
  YGM_ASSERT_RELEASE(ser_idxs_opt.has_value());
  auto series_idxs = ser_idxs_opt.value();
  YGM_ASSERT_RELEASE(!series_idxs.empty());

  // Comparator for the priority queue (inverted for min-heap behavior)
  auto row_comp =
    [&comp](const std::vector<data_types>& a,
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
                      std::vector<std::vector<data_types>>, decltype(row_comp)>
    min_heap(row_comp);

  for_all_func(
    [&](auto rid) {
      auto source_row = pdata->get(ser_idxs_opt.value(), rid);
      std::vector<data_types> row;
      row.reserve(source_row.size());
      for (const auto& el : source_row) {
        data_types dt = std::visit(
          [](const auto& val) -> data_types {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, int64_t>) {
              return static_cast<size_t>(val);
            } else if constexpr (std::is_same_v<T, std::string_view>) {
              return std::string(val);
            } else {
              return val;
            }
          },
          el);
        row.push_back(dt);
      }
      min_heap.push(std::move(row));
      if (min_heap.size() > k) {
        min_heap.pop();  // Remove the smallest
      }
    },
    where);

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
    [comp, k, row_comp](const std::vector<std::vector<data_types>>& va,
                        const std::vector<std::vector<data_types>>& vb) {
      std::vector<std::vector<data_types>> out(va.begin(), va.end());
      out.insert(out.end(), vb.begin(), vb.end());

      std::sort(out.begin(), out.end(), row_comp);
      if (out.size() > k) {
        out.resize(k);
      }

      return out;
    },
    m_comm);
  return to_return;
}

}  // namespace metalldata
