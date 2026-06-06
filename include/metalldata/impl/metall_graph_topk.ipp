#pragma once
#include <metalldata/metall_graph.hpp>
#include <functional>
#include <queue>
#include "ygm/detail/collective.hpp"
#include "ygm/utility/assert.hpp"

namespace metalldata {

template <typename Compare>
std::vector<std::vector<metall_graph::count_types>> metall_graph::topk(
  size_t k, const series_name& ser_name,
  const std::vector<series_name>& ser_inc, Compare comp,
  const where_clause& where) {
  record_store_type* pdata;

  if (!has_series(ser_name)) {
    m_comm.cerr0() << "Warning: series " << ser_name << " does not exist.";
    return {};
  }
  bool is_edge = ser_name.is_edge_series();
  bool is_node = ser_name.is_node_series();

  if (!(is_edge ^ is_node)) {
    m_comm.cerr0() << "Warning: series type is unknown.";
    return {};
  }

  // pdata = is_edge ? m_pedges : m_pnodes;

  // we make sure that the compared column is element 0. This
  // also guarantees that the vector is not empty.
  std::vector<series_name> ser_inc_unq{ser_name};

  for (const auto& ser : ser_inc) {
    if ((is_edge && !ser.is_edge_series()) ||
        (!is_edge && !ser.is_node_series())) {
      m_comm.cerr0() << "Warning: invalid series " << ser << " ignored.";
      continue;
    } else {
      ser_inc_unq.emplace_back(ser);
    }
  }

  std::vector<std::optional<node_series_idx_type>> node_o_idxs{};
  std::vector<std::optional<edge_series_idx_type>> edge_o_idxs{};

  if (is_edge) {
    edge_o_idxs = priv_local_find_edge_series(ser_inc_unq);
  } else {
    node_o_idxs = priv_local_find_node_series(ser_inc_unq);
  }

  // auto ser_idxs_opt = pdata->find_series(ser_inc_unq);
  // YGM_ASSERT_RELEASE(ser_idxs_opt.has_value());
  // auto series_idxs = ser_idxs_opt.value();
  // YGM_ASSERT_RELEASE(!series_idxs.empty());

  // Comparator for the priority queue (inverted for min-heap behavior)
  auto row_comp =
    [&comp](const std::vector<count_types>& a,
            const std::vector<count_types>& b) {
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
  std::priority_queue<std::vector<count_types>,
                      std::vector<std::vector<count_types>>, decltype(row_comp)>
    min_heap(row_comp);

  auto process_row = [&](auto rid) {
    // auto                     rid = static_cast<record_id_type>(rid_);
    std::vector<series_types> source_row{};
    using R = std::decay_t<decltype(rid)>;
    if constexpr (std::is_same_v<R, local_edge_idx_type>) {
      bool issued_inv_series_warning = false;

      m_comm.cerr0() << "edge_o_idxs.size() = " << edge_o_idxs.size() << "\n";
      for (const auto& el : edge_o_idxs) {
        if (el.has_value()) {
          source_row.emplace_back(priv_local_get_edge_field(el.value(), rid)
                                    .value_or(std::monostate{}));
        } else {
          if (!issued_inv_series_warning) {
            m_comm.cerr0()
              << "Warning: invalid series; treating data as missing";
            issued_inv_series_warning = true;
          }
          source_row.emplace_back(std::monostate{});
        }
      }
    } else if constexpr (std::is_same_v<R, local_node_idx_type>) {
      bool issued_inv_series_warning = false;

      for (const auto& el : node_o_idxs) {
        if (el.has_value()) {
          source_row.emplace_back(priv_local_get_node_field(el.value(), rid)
                                    .value_or(std::monostate()));
        } else {
          if (!issued_inv_series_warning) {
            m_comm.cerr0()
              << "Warning: invalid series; treating data as missing";
            issued_inv_series_warning = true;
          }
          source_row.emplace_back(std::monostate{});
        }
      }
    } else {
      static_assert(std::is_same_v<R, void>, "Fatal: unknown row index type");
    }

    // m_comm.cerr0() << "source row size = " << source_row.size();
    std::vector<count_types> row;
    row.reserve(source_row.size());
    for (const auto& el : source_row) {
      count_types dt = std::visit(
        [](const auto& val) -> count_types {
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
  std::vector<std::vector<count_types>> topk_rows;
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
    [comp, k, row_comp](const std::vector<std::vector<count_types>>& va,
                        const std::vector<std::vector<count_types>>& vb) {
      std::vector<std::vector<count_types>> out(va.begin(), va.end());
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
