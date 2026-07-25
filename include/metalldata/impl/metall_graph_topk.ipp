// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <expected>
#include <format>
#include <metalldata/metall_graph.hpp>
#include <functional>
#include <queue>
#include <ygm/detail/collective.hpp>
#include <ygm/utility/assert.hpp>

namespace metalldata {

template <typename Compare>
metalldata::result<
  std::vector<std::vector<metalldata::metall_graph::data_types>>>
metall_graph::topk(size_t k, const series_name& ser_name,
                   const std::vector<series_name>& ser_inc, Compare comp,
                   const where_clause& where) {
  record_store_type* pdata;
  result<std::vector<std::vector<metalldata::metall_graph::data_types>>>
    to_return;

  if (!has_series(ser_name)) {
    return std::unexpected(
      std::format("Series {} does not exist", ser_name.qualified()));
  }
  bool is_edge = ser_name.is_edge_series();
  bool is_node = ser_name.is_node_series();

  if (!(is_edge ^ is_node)) {
    return std::unexpected("Series type is unknown.");
  }

  // we make sure that the compared column is element 0. This
  // also guarantees that the vector is not empty.
  std::vector<series_name> ser_inc_unq{ser_name};

  for (const auto& ser : ser_inc) {
    if ((is_edge && !ser.is_edge_series()) ||
        (!is_edge && !ser.is_node_series())) {
      to_return.add_warning("invalid series {} ignored", ser.qualified());
      continue;
    } else {
      ser_inc_unq.emplace_back(ser);
    }
  }

  std::vector<node_series_idx_type> node_idxs{};
  std::vector<edge_series_idx_type> edge_idxs{};

  if (is_edge) {
    for (const auto& idx : pl_find_edge_series(ser_inc_unq)) {
      if (!idx.has_value()) {
        to_return.add_warning("found invalid edge series index; skipping");
      } else {
        edge_idxs.emplace_back(idx.value());
      }
    }
  } else {
    for (const auto& idx : pl_find_node_series(ser_inc_unq)) {
      if (!idx.has_value()) {
        to_return.add_warning("found invalid node series index; skipping");
      } else {
        node_idxs.emplace_back(idx.value());
      }
    }
  }

  // Comparator for the priority queue (inverted for min-heap behavior)
  auto row_comp = [&comp](const std::vector<data_types>& a,
                          const std::vector<data_types>& b) {
    YGM_ASSERT_RELEASE(!a.empty() && !b.empty());
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

  auto process_row = [&](auto rid) {
    // auto                     rid = static_cast<record_id_type>(rid_);
    std::vector<series_types> source_row{};
    using R = std::decay_t<decltype(rid)>;
    if constexpr (std::is_same_v<R, local_edge_idx_type>) {
      for (const auto& el : edge_idxs) {
        source_row.emplace_back(
          pl_get_edge_field(el, rid).value_or(std::monostate{}));
      }
    } else if constexpr (std::is_same_v<R, local_node_idx_type>) {
      for (const auto& el : node_idxs) {
        source_row.emplace_back(
          pl_get_node_field(el, rid).value_or(std::monostate()));
      }
    } else {
      static_assert(std::is_same_v<R, void>, "Fatal: unknown row index type");
    }

    std::vector<data_types> row;
    row.reserve(source_row.size());
    for (const auto& el : source_row) {
      data_types dt = std::visit(
        [](const auto& val) -> data_types {
          using T = std::decay_t<decltype(val)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
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
      min_heap.pop();
    }
  };

  if (is_edge) {
    priv_for_all_edges(process_row, where);
  } else {
    priv_for_all_nodes(process_row, where);
  }

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

  auto topk_outcome = ygm::all_reduce(
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
  to_return = topk_outcome;
  return to_return;
}

}  // namespace metalldata
