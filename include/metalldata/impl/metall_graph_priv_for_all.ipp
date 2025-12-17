#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
// The following for_all functions take a function that
// is passed the index as a parameter:
// Fn: [](record_id_type record_id) {}
// TODO: need to accept node where clauses. This is tricky. Leave for Roger.
template <typename Fn>
void metall_graph::priv_for_all_edges(
  Fn func, const metall_graph::where_clause& where) const {
  // take the where clause. Convert the where clause variables to
  // a vector of series indices. If it's missing, throw runtime.
  //

  if (where.empty()) {
    m_pedges->for_all_rows(func);
    return;
  }
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
  auto wrapper  = [&](size_t row_index) {
    std::vector<data_types> var_data;
    var_data.reserve(var_idxs.size());
    for (auto series_idx : var_idxs) {
      auto val = m_pedges->get_dynamic(series_idx, row_index);
      std::visit(
        [&var_data](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, int64_t>) {
            var_data.push_back(size_t(v));
          } else if constexpr (std::is_same_v<T, std::string_view>) {
            var_data.push_back(std::string(v));
          } else {
            var_data.push_back(v);
          }
        },
        val);
    }

    if (where.evaluate(var_data)) {
      func(row_index);
    }
  };
  if (where.good()) {
    m_pedges->for_all_rows(wrapper);
  }
};

// for_all_nodes lambda takes a row index.
template <typename Fn>
void metall_graph::priv_for_all_nodes(
  Fn func, const metall_graph::where_clause& where) const {
  if (where.empty()) {
    m_pnodes->for_all_rows([&](auto row_index) { func(row_index); });
    return;
  }
  if (where.is_node_clause()) {
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
      std::vector<data_types> var_data;
      var_data.reserve(var_idxs.size());
      bool missing_field = false;
      for (auto series_idx : var_idxs) {
        if (m_pnodes->is_none(series_idx, row_index)) {
          missing_field = true;
          break;
        }
        auto val = m_pnodes->get_dynamic(series_idx, row_index);

        std::visit(
          [&var_data, &missing_field](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int64_t>) {
              var_data.push_back(size_t(v));
            } else if constexpr (std::is_same_v<T, std::string_view>) {
              var_data.push_back(std::string(v));
            } else {
              var_data.push_back(v);
            }
          },
          val);
      }

      if (!missing_field && where.evaluate(var_data)) {
        func(row_index);
      }
    };

    m_pnodes->for_all_rows(wrapper);
  } else if (where.is_edge_clause()) {
    auto u_col_idx = m_pedges->find_series(U_COL.unqualified());
    auto v_col_idx = m_pedges->find_series(V_COL.unqualified());

    ygm::container::set<std::string> nodeset(m_comm);
    priv_for_all_edges(
      [&](record_id_type record_idx) {
        auto u = m_pedges->get<std::string_view>(u_col_idx, record_idx);
        auto v = m_pedges->get<std::string_view>(v_col_idx, record_idx);

        nodeset.async_insert(std::string(u));
        nodeset.async_insert(std::string(v));
      },
      where);

    std::unordered_map<std::string, record_id_type> node_to_id;
    auto node_col_idx = m_pnodes->find_series(NODE_COL.unqualified());
    m_pnodes->for_all_rows([&](record_id_type rid) {
      auto name = m_pnodes->get<std::string_view>(node_col_idx, rid);

      node_to_id[std::string(name)] = rid;
    });

    for (const auto& node : nodeset) {
      // throw an exception if the node is not in our node dataframe.
      func(node_to_id.at(node));
    }
  }
}
}  // namespace metalldata