#include <metalldata/metall_graph.hpp>
#define BOOST_JSON_SRC_HPP  // This is a temp hack until YGM removes the src.hpp
                            // inclusion
#include <ygm/utility/boost_json.hpp>

namespace metalldata {
// Returns a set of randomly-selected edge indices on this rank.
// The indices are uniformly chosen from all the edges in the graph.
// Only indices that exist on this rank are returned in the set.
std::unordered_set<metall_graph::record_id_type>
metall_graph::priv_random_edge_idx(const size_t k, const where_clause& where) {
  std::unordered_set<metall_graph::record_id_type> local_data;

  // this builds a set of local edges.
  std::unordered_set<record_id_type> filtered_edge_id_set;
  priv_for_all_edges([&](const auto& rid) { filtered_edge_id_set.insert(rid); },
                     where);

  // filtered_edge_ids is a 0..n vector of rids that represent edges passing the
  // where clause.
  std::vector<record_id_type> filtered_edge_ids(filtered_edge_id_set.begin(),
                                                filtered_edge_id_set.end());

  // since we've already done the where, don't use num_edges().
  size_t n_edges     = filtered_edge_ids.size();
  size_t global_ne   = ygm::sum(n_edges, m_comm);
  size_t sample_size = std::min(global_ne, k);

  // we prefix_sum the size of the number of edges passing the where clause.
  size_t lower_bound = ygm::prefix_sum(n_edges, m_comm);

  m_comm.barrier();

  std::vector<size_t> selected_edges;
  selected_edges.reserve(sample_size);

  // rank0 will fill selected_edges with indices from 0..global_ne based on a
  // uniform distribution.
  if (m_comm.rank0()) {
    std::unordered_set<size_t>            selected_edge_set;
    std::random_device                    rd;
    std::mt19937                          gen(rd());
    std::uniform_int_distribution<size_t> dist(0, global_ne - 1);

    while (selected_edge_set.size() < sample_size) {
      size_t random_edge = dist(gen);
      selected_edge_set.insert(random_edge);
    }

    selected_edges.assign(selected_edge_set.begin(), selected_edge_set.end());
  }

  // send the selected_edges to all nodes.
  ygm::bcast(selected_edges, 0, m_comm);

  for (const auto& selected_edge : selected_edges) {
    // this will cause problems if we're greter than 2^63 edges....
    int64_t local_idx =
      static_cast<int64_t>(selected_edge) - static_cast<int64_t>(lower_bound);

    if (local_idx >= 0 && local_idx < static_cast<int64_t>(n_edges)) {
      // this is our rank. Process.
      record_id_type local_edge = filtered_edge_ids.at(local_idx);
      YGM_ASSERT_RELEASE(!local_data.contains(local_edge));
      local_data.insert(local_edge);
    }
  }

  return local_data;
}

// Creates a column of bool where true values indicate that the edge has been
// selected during the random sample.
metall_graph::return_code metall_graph::sample_edges(
  const metall_graph::series_name& series_name, size_t k,
  const metall_graph::where_clause& where) {
  metall_graph::return_code to_return;

  if (has_edge_series(series_name)) {
    to_return.error =
      std::format("Series {} already exists", series_name.qualified());
    return to_return;
  }

  auto local_data = priv_random_edge_idx(k, where);
  std::unordered_map<metall_graph::record_id_type, bool> local_map{};

  for (const auto rid : local_data) {
    local_map[rid] = true;
  }
  m_comm.barrier();
  priv_set_edge_column_by_idx(series_name, local_map);
  return to_return;
}

bjsn::array metall_graph::select_sample_edges(
  size_t k, const std::vector<metall_graph::series_name>& metadata,
  const metall_graph::where_clause& where) {
  auto local_data = priv_random_edge_idx(k, where);

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

}  // namespace metalldata