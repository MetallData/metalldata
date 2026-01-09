#include <metalldata/metall_graph.hpp>
#define BOOST_JSON_SRC_HPP  // This is a temp hack until YGM removes the src.hpp
                            // inclusion
#include <ygm/utility/boost_json.hpp>

namespace metalldata {
// Returns randomly-selected record ids (edges when sample_edges is true, nodes
// otherwise) that exist on this rank. Selection is uniform across all ranks.
std::unordered_set<metall_graph::record_id_type> metall_graph::priv_random_idx(
  const std::unordered_set<record_id_type>& filtered_ids_set, const size_t k,
  uint64_t seed) {
  std::vector<record_id_type> filtered_ids(filtered_ids_set.begin(),
                                           filtered_ids_set.end());

  size_t local_count  = filtered_ids.size();
  size_t global_count = ygm::sum(local_count, m_comm);
  size_t sample_size  = std::min(global_count, k);
  size_t lower_bound  = ygm::prefix_sum(local_count, m_comm);

  m_comm.barrier();

  std::vector<size_t> selected_indices;
  selected_indices.reserve(sample_size);

  if (m_comm.rank0()) {
    std::unordered_set<size_t>            selection;
    std::mt19937                          gen(seed);
    std::uniform_int_distribution<size_t> dist(0, global_count - 1);

    while (selection.size() < sample_size) {
      selection.insert(dist(gen));
    }

    selected_indices.assign(selection.begin(), selection.end());
  }

  ygm::bcast(selected_indices, 0, m_comm);

  std::unordered_set<record_id_type> local_data;

  for (const auto idx : selected_indices) {
    if ((idx >= lower_bound) && (idx < lower_bound + local_count)) {
      // local idx is guaranteed to be >= 0
      record_id_type rid = filtered_ids.at(idx - lower_bound);
      YGM_ASSERT_RELEASE(!local_data.contains(rid));
      local_data.insert(rid);
    }
  }

  return local_data;
}

// std::unordered_set<metall_graph::record_id_type>
// metall_graph::priv_random_edge_idx(const size_t k, uint64_t seed,
//                                    const where_clause& where) {
//   return priv_random_idx(true, k, seed, where);
// }

// Creates a column of bool where true values indicate that the edge has been
// selected during the random sample.
metall_graph::return_code metall_graph::sample_edges(
  const metall_graph::series_name& series_name, size_t k,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  metall_graph::return_code to_return;

  if (has_edge_series(series_name)) {
    to_return.error =
      std::format("Series {} already exists", series_name.qualified());
    return to_return;
  }

  uint64_t seed;
  if (optseed.has_value()) {
    seed = optseed.value();
  } else {
    std::random_device rd;
    seed = rd();
  }

  std::unordered_set<record_id_type> filtered_ids_set;
  priv_for_all_edges([&](record_id_type rid) { filtered_ids_set.insert(rid); },
                     where);

  auto local_data = priv_random_idx(filtered_ids_set, k, seed);
  std::unordered_map<metall_graph::record_id_type, bool> local_map{};

  for (const auto rid : local_data) {
    local_map[rid] = true;
  }
  m_comm.barrier();
  priv_set_column_by_idx(true, series_name, local_map);
  return to_return;
}

bjsn::array metall_graph::select_sample_edges(
  size_t k, const std::vector<metall_graph::series_name>& metadata,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  uint64_t seed;
  if (optseed.has_value()) {
    seed = optseed.value();
  } else {
    std::random_device rd;
    seed = rd();
  }

  std::unordered_set<record_id_type> filtered_ids_set;
  priv_for_all_edges([&](record_id_type rid) { filtered_ids_set.insert(rid); },
                     where);
  auto local_data = priv_random_idx(filtered_ids_set, k, seed);

  std::vector<std::string> unqual_metadata;
  for (const auto& sn : metadata) {
    unqual_metadata.push_back(std::string(sn.unqualified()));
  }

  std::unordered_map<series_index_type, series_name> idx_to_name;
  for (const auto& sname : metadata) {
    auto idx = m_pedges->find_series(sname.unqualified());
    if (idx == std::numeric_limits<size_t>::max()) {
      return {};
    }
    idx_to_name[idx] = sname;
  }

  bjsn::array rows;

  for (const auto& rid : local_data) {
    bjsn::object row;
    for (const auto& [idx, sname] : idx_to_name) {
      auto val = m_pedges->get_dynamic(idx, rid);

      std::visit(
        [&](auto&& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            row[sname.unqualified()] = std::string(v);
          } else if constexpr (std::is_same_v<T, std::monostate>) {
            row[sname.unqualified()] = nullptr;
          } else {
            row[sname.unqualified()] = v;
          }
        },
        val);
    }
    rows.emplace_back(row);
  }
  std::vector<bjsn::array> everything(m_comm.size() - 1);  // don't need rank 0
  static auto&             s_everything = everything;
  m_comm.cf_barrier();
  if (!m_comm.rank0()) {
    m_comm.async(
      0,
      [](const bjsn::array& rank_data, int rank) {
        (s_everything)[rank - 1] = rank_data;
      },
      rows, m_comm.rank());
  }

  m_comm.barrier();

  if (m_comm.rank0()) {
    for (auto& el : everything) {
      rows.insert(rows.end(), el.begin(), el.end());
      el.clear();
    }
  }

  m_comm.barrier();

  return rows;
}

// Nodes below.

// // Returns a set of randomly-selected edge indices on this rank.
// // The indices are uniformly chosen from all the edges in the graph.
// // Only indices that exist on this rank are returned in the set
// std::unordered_set<metall_graph::record_id_type>
// metall_graph::priv_random_node_idx(const size_t k, uint64_t seed,
//                                    const where_clause& where) {
//   return priv_random_idx(false, k, seed, where);
// }

// Creates a column of bool where true values indicate that the edge has been
// selected during the random sample.
metall_graph::return_code metall_graph::sample_nodes(
  const metall_graph::series_name& series_name, size_t k,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  metall_graph::return_code to_return;

  if (has_node_series(series_name)) {
    to_return.error =
      std::format("Series {} already exists", series_name.qualified());
    return to_return;
  }

  uint64_t seed;
  if (optseed.has_value()) {
    seed = optseed.value();
  } else {
    std::random_device rd;
    seed = rd();
  }

  std::unordered_set<record_id_type> filtered_ids_set;
  priv_for_all_nodes([&](record_id_type rid) { filtered_ids_set.insert(rid); },
                     where);

  auto local_data = priv_random_idx(filtered_ids_set, k, seed);
  std::unordered_map<metall_graph::record_id_type, bool> local_map{};

  for (const auto rid : local_data) {
    local_map[rid] = true;
  }
  m_comm.barrier();
  priv_set_column_by_idx(false, series_name, local_map);
  return to_return;
}

bjsn::array metall_graph::select_sample_nodes(
  size_t k, const std::vector<metall_graph::series_name>& metadata,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  uint64_t seed;
  if (optseed.has_value()) {
    seed = optseed.value();
  } else {
    std::random_device rd;
    seed = rd();
  }

  std::unordered_set<record_id_type> filtered_ids_set;
  priv_for_all_nodes([&](record_id_type rid) { filtered_ids_set.insert(rid); },
                     where);

  auto local_data = priv_random_idx(filtered_ids_set, k, seed);

  std::vector<std::string> unqual_metadata;
  for (const auto& sn : metadata) {
    unqual_metadata.push_back(std::string(sn.unqualified()));
  }

  std::unordered_map<series_index_type, series_name> idx_to_name;
  for (const auto& sname : metadata) {
    auto idx = m_pnodes->find_series(sname.unqualified());
    if (idx == std::numeric_limits<size_t>::max()) {
      return {};
    }
    idx_to_name[idx] = sname;
  }

  bjsn::array rows;

  for (const auto& rid : local_data) {
    bjsn::object row;
    for (const auto& [idx, sname] : idx_to_name) {
      auto val = m_pnodes->get_dynamic(idx, rid);

      std::visit(
        [&](auto&& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            row[sname.unqualified()] = std::string(v);
          } else if constexpr (std::is_same_v<T, std::monostate>) {
            row[sname.unqualified()] = nullptr;
          } else {
            row[sname.unqualified()] = v;
          }
        },
        val);
    }
    rows.emplace_back(row);
  }
  std::vector<bjsn::array> everything(m_comm.size() - 1);  // don't need rank 0
  static auto&             s_everything = everything;
  m_comm.cf_barrier();
  if (!m_comm.rank0()) {
    m_comm.async(
      0,
      [](const bjsn::array& rank_data, int rank) {
        (s_everything)[rank - 1] = rank_data;
      },
      rows, m_comm.rank());
  }

  m_comm.barrier();

  if (m_comm.rank0()) {
    for (auto& el : everything) {
      rows.insert(rows.end(), el.begin(), el.end());
      el.clear();
    }
  }

  m_comm.barrier();

  return rows;
}

}  // namespace metalldata