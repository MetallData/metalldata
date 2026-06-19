#include <metalldata/metall_graph.hpp>
#include <utility>
#include "ygm/utility/assert.hpp"
#include <metalldata/detail/random_sample.hpp>
#include <variant>

namespace metalldata {
using metadata_t = std::vector<metall_graph::data_types>;

// Creates a column of bool where true values indicate that the edge has been
// selected during the random sample.
result<> metall_graph::sample_edges(
  const metall_graph::series_name& series_name, size_t k,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  if (has_edge_series(series_name)) {
    return std::unexpected(
      std::format("series {} already exists", series_name.qualified()));
  }

  uint64_t seed = optseed.value_or(0);

  std::vector<local_edge_idx_type> filtered_ids;
  priv_for_all_edges(
    [&](local_edge_idx_type eid) { filtered_ids.emplace_back(eid); }, where);

  auto local_data = random_sample(filtered_ids, k, seed, m_comm);
  std::unordered_map<local_edge_idx_type, bool> local_map{};

  for (const auto id : local_data) {
    local_map[id] = true;
  }
  m_comm.barrier();
  priv_set_edge_column_by_idx(series_name, local_map);
  return result<>{};
}

result<ygm::container::bag<metadata_t>> metall_graph::select_sample_edges(
  size_t k, const std::vector<metall_graph::series_name>& metadata,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  uint64_t seed = optseed.value_or(0);

  result<ygm::container::bag<metadata_t>> to_return({m_comm});

  ygm::container::bag<metadata_t>& selected_edges = to_return.value();

  // filtered ids holds the list of eids on each rank that pass the where clause
  std::vector<local_edge_idx_type> filtered_ids;
  priv_for_all_edges(
    [&](local_edge_idx_type eid) { filtered_ids.emplace_back(eid); }, where);

  auto local_eidxs = random_sample(filtered_ids, k, seed, m_comm);

  auto ser_idxs_o = pl_find_edge_series(metadata);

  std::vector<edge_series_idx_type> ser_idxs;
  for (const auto i : ser_idxs_o) {
    if (!i.has_value()) {
      continue;
    }
    ser_idxs.emplace_back(i.value());
  }

  for (const auto& eid : local_eidxs) {
    auto                    row_edgedata = pl_get_edge_fields(ser_idxs, eid);
    std::vector<data_types> row_data;
    for (const auto& e : row_edgedata) {
      if (!e.has_value()) {
        continue;
      }
      auto d = priv_series_to_data_type(e.value());
      row_data.emplace_back(d);
    }
    selected_edges.local_insert(row_data);
  }

  return to_return;
}

// Nodes below.

// // Returns a set of randomly-selected edge indices on this rank.
// // The indices are uniformly chosen from all the edges in the graph.
// // Only indices that exist on this rank are returned in the set
// std::unordered_set<local_node_idx_type>
// metall_graph::priv_random_node_idx(const size_t k, uint64_t seed,
//                                    const where_clause& where) {
//   return priv_random_idx(false, k, seed, where);
// }

// Creates a column of bool where true values indicate that the edge has been
// selected during the random sample.
result<> metall_graph::sample_nodes(
  const metall_graph::series_name& series_name, size_t k,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  if (has_node_series(series_name)) {
    return std::unexpected(
      std::format("Series {} already exists", series_name.qualified()));
  }

  uint64_t seed = optseed.value_or(0);

  std::vector<local_node_idx_type> filtered_ids;
  priv_for_all_nodes(
    [&](local_node_idx_type nid) { filtered_ids.emplace_back(nid); }, where);

  auto local_data = random_sample(filtered_ids, k, seed, m_comm);
  std::unordered_map<local_node_idx_type, bool> local_map{};

  for (const auto rid : local_data) {
    local_map[rid] = true;
  }
  m_comm.barrier();
  priv_set_node_column_by_idx(series_name, local_map);
  return result<>{};
}

result<ygm::container::bag<metadata_t>> metall_graph::select_sample_nodes(
  size_t k, const std::vector<metall_graph::series_name>& metadata,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  uint64_t seed = optseed.value_or(0);

  result<ygm::container::bag<metadata_t>> to_return({m_comm});
  ygm::container::bag<metadata_t>&        selected_rows = to_return.value();

  std::vector<local_node_idx_type> filtered_ids;
  priv_for_all_nodes(
    [&](local_node_idx_type nid) { filtered_ids.emplace_back(nid); }, where);

  auto local_nidxs = random_sample(filtered_ids, k, seed, m_comm);

  auto ser_idxs_o = pl_find_node_series(metadata);

  std::vector<node_series_idx_type> ser_idxs;

  for (const auto i : ser_idxs_o) {
    if (!i.has_value()) {
      continue;
    }
    ser_idxs.emplace_back(i.value());
  }

  for (const auto& nid : local_nidxs) {
    auto                    row_nodedata = pl_get_node_fields(ser_idxs, nid);
    std::vector<data_types> row_data;
    for (const auto& e : row_nodedata) {
      if (!e.has_value()) {
        continue;
      }
      auto d = priv_series_to_data_type(e.value());
      row_data.emplace_back(d);
    }
    selected_rows.local_insert(row_data);
  }

  return to_return;
}

}  // namespace metalldata