#pragma once
#include <metalldata/metall_graph.hpp>
#include <ygm/utility/assert.hpp>

namespace metalldata {

template <typename Fn>
void metall_graph::priv_for_all_edges_nwhere(
  Fn func, const metall_graph::where_clause& where) const {
  YGM_ASSERT_RELEASE(where.is_node_clause());
  // 1. Compute the set of nodes that satisfy the node where clause.
  ygm::container::set<std::string> nodeset(m_comm);
  priv_for_all_nodes_nwhere(
    [&](local_node_idx_type nid) {
      auto u = priv_local_get_node_label(nid);
      if (u.has_value()) {
        nodeset.async_insert(std::string(u.value()));
      }
    },
    where);
  m_comm.barrier();

  // 2. Gather list of nodes needed by rank local edges
  std::set<std::string> nodes_i_need;
  priv_for_all_edges([&](local_edge_idx_type eid) {
    auto ouv = priv_local_get_edge_uv_labels(eid);
    if (ouv.has_value()) {
      auto [u, v] = ouv.value();
      nodes_i_need.insert(std::string(u));
      nodes_i_need.insert(std::string(v));
    }
  });
  std::set<std::string> nodes_alive = nodeset.gather_values(nodes_i_need);

  // 3. Compute the set of edges that are incident on those nodes.
  priv_for_all_edges([&](local_edge_idx_type eid) {
    auto ouv = priv_local_get_edge_uv_labels(eid);
    if (ouv.has_value()) {
      auto [u, v] = ouv.value();
      if (nodes_alive.contains(std::string(u)) &&
          nodes_alive.contains(std::string(v))) {
        func(eid);
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

template <typename Fn>
void metall_graph::priv_for_all_edges(Fn func) const {
  m_pedges->for_all_rows(
    [&](record_id_type rid) { func(local_edge_idx_type{rid}); });
}

// The following for_all functions take a function that
// is passed the index as a parameter:
// Fn: [](record_id_type record_id) {}
template <typename Fn>
void metall_graph::priv_for_all_edges(
  Fn func, const metall_graph::where_clause& where) const {
  if (where.empty()) {
    priv_for_all_edges(func, where);
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

  // 1. compute the set of edges that satisfy the edge where clause & save
  // vertex labels
  ygm::container::set<std::string> nodeset(m_comm);
  priv_for_all_edges_ewhere(
    [&](local_edge_idx_type eid) {
      auto ouv = priv_local_get_edge_uv_labels(eid);
      YGM_ASSERT_RELEASE(ouv.has_value());
      auto [u, v] = ouv.value();
      nodeset.async_insert(std::string(u));
      nodeset.async_insert(std::string(v));
    },
    where);

  // 2. Compute node ids from vertex labels
  for (const auto& node : nodeset) {
    auto opsa = priv_local_node_find(node);
    YGM_ASSERT_RELEASE(opsa.has_value());
    func(local_node_idx_type{opsa.value()});
  }
}

template <typename Fn>
void metall_graph::priv_for_all_nodes(Fn func) const {
  m_pnodes->for_all_rows(
    [&](record_id_type rid) { func(local_node_idx_type{rid}); });
}

// for_all_nodes lambda takes a row index.
template <typename Fn>
void metall_graph::priv_for_all_nodes(
  Fn func, const metall_graph::where_clause& where) const {
  if (where.empty()) {
    priv_for_all_nodes(func, where);
  } else if (where.is_node_clause()) {
    priv_for_all_nodes_nwhere(func, where);
  } else if (where.is_edge_clause()) {
    priv_for_all_nodes_ewhere(func, where);
  }
}
}  // namespace metalldata