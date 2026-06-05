#pragma once
#include <metalldata/metall_graph.hpp>
#include <ygm/utility/assert.hpp>

namespace metalldata {

template <typename Fn>
void metall_graph::priv_for_all_edges_nwhere(
  Fn func, const metall_graph::where_clause& where) const {
  YGM_ASSERT_RELEASE(where.is_node_clause());
  // TODO: need to accept node where clauses. This is tricky. Leave for Roger.
  ygm::container::set<std::string> nodeset(m_comm);
  priv_for_all_nodes_nwhere(
    [&](local_node_idx_type record_idx) {
      auto u = priv_local_get_node_label(record_idx);
      if (u.has_value()) {
        nodeset.async_insert(std::string(u.value()));
      }
    },
    where);
  m_comm.barrier();

  std::set<std::string> nodes_i_need;
  m_pedges->for_all_rows([&](record_id_type record_idx) {
    auto [u,v] = priv_local_edge_uv(local_edge_idx_type{record_idx});

    if (u.has_value()) {
      nodes_i_need.insert(std::string(u.value()));
    }

    if (v.has_value()) {
      nodes_i_need.insert(std::string(v.value()));
    }
  });

  std::set<std::string> nodes_alive = nodeset.gather_values(nodes_i_need);

  m_pedges->for_all_rows([&](record_id_type record_idx) {
    auto [u,v] = priv_local_edge_uv(local_edge_idx_type{record_idx});
    if (u.has_value() && v.has_value()) {
      if (nodes_alive.contains(std::string(u.value())) &&
          nodes_alive.contains(std::string(v.value()))) {
        func(local_edge_idx_type{record_idx});
      }
    }
  });
}

template <typename Fn>
void metall_graph::priv_for_all_edges_ewhere(
  Fn func, const metall_graph::where_clause& where) const {
  // take the where clause. Convert the where clause variables to
  // a vector of series indices. If it's missing, throw runtime.
  //
  YGM_ASSERT_RELEASE(where.is_edge_clause());

  std::vector<std::string> str_series_names;
  str_series_names.reserve(where.series_names().size());
  for (const auto& n : where.series_names()) {
    str_series_names.emplace_back(n.unqualified());
  }
  auto var_idxs_o = m_pedges->find_series(str_series_names);
  if (!var_idxs_o.has_value()) {
    return;
  }

  auto var_idxs = var_idxs_o.value();
  auto wrapper = [&](size_t row_index) {
    std::vector<series_types> var_data;
    var_data.reserve(var_idxs.size());
    bool missing_field = false;
    for (auto series_idx : var_idxs) {
      if (m_pedges->is_none(series_idx, row_index)) {
        missing_field = true;
        break;
      }
      auto val_o = m_pedges->get_dynamic(series_idx, row_index);
      if (!val_o.has_value()) {
        continue;
      }
      var_data.push_back(val_o.value());
    }

    if (!missing_field && where.evaluate(var_data)) {
      func(local_edge_idx_type{row_index});
    }
  };
  if (where.good()) {
    m_pedges->for_all_rows(wrapper);
  }
}

// The following for_all functions take a function that
// is passed the index as a parameter:
// Fn: [](record_id_type record_id) {}
template <typename Fn>
void metall_graph::priv_for_all_edges(
  Fn func, const metall_graph::where_clause& where) const {
  if (where.empty()) {
    m_pedges->for_all_rows(
      [&](record_id_type rid) { func(local_edge_idx_type{rid}); });
  } else if (where.is_node_clause()) {
    priv_for_all_edges_nwhere(func, where);
  } else if (where.is_edge_clause()) {
    priv_for_all_edges_ewhere(func, where);
  }
};

template <typename Fn>
void metall_graph::priv_for_all_nodes_nwhere(
  Fn func, const metall_graph::where_clause& where) const {
  YGM_ASSERT_RELEASE(where.is_node_clause());
  std::vector<std::string> str_series_names;
  str_series_names.reserve(where.series_names().size());
  for (auto n : where.series_names()) {
    str_series_names.emplace_back(n.unqualified());
  }
  auto var_idxs_o = m_pnodes->find_series(str_series_names);
  if (!var_idxs_o.has_value()) {
    return;
  }
  auto var_idxs = var_idxs_o.value();

  auto wrapper = [&](size_t row_index) {
    std::vector<series_types> var_data;
    var_data.reserve(var_idxs.size());
    bool missing_field = false;
    for (auto series_idx : var_idxs) {
      if (m_pnodes->is_none(series_idx, row_index)) {
        missing_field = true;
        break;
      }
      auto val_o = m_pnodes->get_dynamic(series_idx, row_index);
      if (!val_o.has_value()) {
        continue;
      }
      var_data.push_back(val_o.value());
    }

    if (!missing_field && where.evaluate(var_data)) {
      func(local_node_idx_type{row_index});
    }
  };

  m_pnodes->for_all_rows(wrapper);
}

template <typename Fn>
void metall_graph::priv_for_all_nodes_ewhere(
  Fn func, const metall_graph::where_clause& where) const {
  YGM_ASSERT_RELEASE(where.is_edge_clause());
  auto u_col_idx_o = m_pedges->find_series(U_COL.unqualified());
  auto v_col_idx_o = m_pedges->find_series(V_COL.unqualified());
  if (!u_col_idx_o.has_value() || !v_col_idx_o.has_value()) {
    return;
  }
  auto u_col_idx = u_col_idx_o.value();
  auto v_col_idx = v_col_idx_o.value();

  ygm::container::set<std::string> nodeset(m_comm);
  priv_for_all_edges_ewhere(
    [&](local_edge_idx_type eid) {
      auto [u, v] = priv_local_edge_uv(eid);
      YGM_ASSERT_RELEASE(u.has_value());
      YGM_ASSERT_RELEASE(v.has_value());
      nodeset.async_insert(std::string(u.value()));
      nodeset.async_insert(std::string(v.value()));
    },
    where);

  for (const auto& node : nodeset) {
    auto opsa = priv_local_node_find(node);
    YGM_ASSERT_RELEASE(opsa.has_value());
    func(local_node_idx_type{opsa.value()});
  }
}

// for_all_nodes lambda takes a row index.
template <typename Fn>
void metall_graph::priv_for_all_nodes(
  Fn func, const metall_graph::where_clause& where) const {
  if (where.empty()) {
    m_pnodes->for_all_rows(
      [&](record_id_type rid) { func(local_node_idx_type{rid}); });
  } else if (where.is_node_clause()) {
    priv_for_all_nodes_nwhere(func, where);
  } else if (where.is_edge_clause()) {
    priv_for_all_nodes_ewhere(func, where);
  }
}
}  // namespace metalldata