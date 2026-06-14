#include <metalldata/metall_graph.hpp>
#include <utility>
#define BOOST_JSON_SRC_HPP  // This is a temp hack until YGM removes the src.hpp
                            // inclusion
#include <ygm/utility/boost_json.hpp>
#include <metalldata/detail/random_sample.hpp>

namespace metalldata {

// Creates a column of bool where true values indicate that the edge has been
// selected during the random sample.
result<> metall_graph::sample_edges(
  const metall_graph::series_name& series_name, size_t k,
  std::optional<uint64_t> optseed, const metall_graph::where_clause& where) {
  if (has_edge_series(series_name)) {
    return std::unexpected(
      std::format("series {} already exists", series_name.qualified()));
  }

  uint64_t seed;
  if (optseed.has_value()) {
    seed = optseed.value();
  } else {
    std::random_device rd;
    seed = rd();
  }

  std::vector<local_edge_idx_type> filtered_ids;
  priv_for_all_edges(
    [&](local_edge_idx_type eid) { filtered_ids.emplace_back(eid); }, where);

  auto local_data = random_sample(m_comm, filtered_ids, k, seed);
  std::unordered_map<local_edge_idx_type, bool> local_map{};

  for (const auto id : local_data) {
    local_map[id] = true;
  }
  m_comm.barrier();
  priv_set_edge_column_by_idx(series_name, local_map);
  return result<>{};
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

  std::vector<local_edge_idx_type> filtered_ids;
  priv_for_all_edges(
    [&](local_edge_idx_type eid) { filtered_ids.emplace_back(eid); }, where);
  auto local_data = random_sample(m_comm, filtered_ids, k, seed);

  std::vector<std::string> unqual_metadata;
  for (const auto& sn : metadata) {
    unqual_metadata.push_back(std::string(sn.unqualified()));
  }

  std::unordered_map<series_index_type, series_name> idx_to_name;
  for (const auto& sname : metadata) {
    auto idx_o = m_pedges->find_series(sname.unqualified());
    if (!idx_o.has_value()) {
      return {};
    }
    auto idx = idx_o.value();
    idx_to_name[idx] = sname;
  }

  bjsn::array rows;

  for (const auto& eid : local_data) {
    bjsn::object row;
    for (const auto& [idx, sname] : idx_to_name) {
      auto val_o = m_pedges->get_dynamic(idx, std::to_underlying(eid));

      if (!val_o.has_value()) {
        continue;
      }
      auto val = val_o.value();
      std::visit(
        [&](auto&& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
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

  uint64_t seed;
  if (optseed.has_value()) {
    seed = optseed.value();
  } else {
    std::random_device rd;
    seed = rd();
  }

  std::vector<local_node_idx_type> filtered_ids;
  priv_for_all_nodes(
    [&](local_node_idx_type nid) { filtered_ids.emplace_back(nid); }, where);

  auto local_data = random_sample(m_comm, filtered_ids, k, seed);
  std::unordered_map<local_node_idx_type, bool> local_map{};

  for (const auto rid : local_data) {
    local_map[rid] = true;
  }
  m_comm.barrier();
  priv_set_node_column_by_idx(series_name, local_map);
  return result<>{};
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

  std::vector<local_node_idx_type> filtered_ids;
  priv_for_all_nodes(
    [&](local_node_idx_type nid) { filtered_ids.emplace_back(nid); }, where);

  auto local_data = random_sample(m_comm, filtered_ids, k, seed);

  std::vector<std::string> unqual_metadata;
  for (const auto& sn : metadata) {
    unqual_metadata.push_back(std::string(sn.unqualified()));
  }

  std::unordered_map<series_index_type, series_name> idx_to_name;
  for (const auto& sname : metadata) {
    auto idx_o = m_pnodes->find_series(sname.unqualified());
    if (!idx_o.has_value()) {
      return {};
    }
    auto idx = idx_o.value();
    idx_to_name[idx] = sname;
  }

  bjsn::array rows;

  for (const auto& nid : local_data) {
    bjsn::object row;
    for (const auto& [idx, sname] : idx_to_name) {
      auto val_o = m_pnodes->get_dynamic(idx, std::to_underlying(nid));

      if (!val_o.has_value()) {
        continue;
      }
      auto val = val_o.value();
      std::visit(
        [&](auto&& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
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