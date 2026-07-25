// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>

namespace metalldata {

result<> metall_graph::assign(series_name                       name,
                              const metall_graph::series_types& val,
                              const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;
  result<> to_return;

  if (has_series(name)) {
    return std::unexpected(
      std::format("series {} already exists", name.qualified()));
  }

  if (name.is_edge_series()) {
    auto pedges_ = m_pedges;
    bool assigned_ok = true;
    std::visit(
      [&assigned_ok, &name, pedges_](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // do nothing
        } else if constexpr (std::is_same_v<T, std::string_view>) {
          pedges_->add_series<std::string_view>(name.unqualified());
        } else if constexpr (std::is_same_v<T, int64_t>) {
          pedges_->add_series<int64_t>(name.unqualified());
        } else if constexpr (std::is_same_v<T, bool>) {
          pedges_->add_series<bool>(name.unqualified());
        } else {
          assigned_ok = false;
        }
      },
      val);

    if (!assigned_ok) {
      return std::unexpected("invalid type for value");
    }

    auto name_idx_o = pl_find_edge_series(name);
    if (!name_idx_o.has_value()) {
      return std::unexpected(
        std::format("series {} not found", name.qualified()));
    }
    auto name_idx = name_idx_o.value();

    auto wrapper = [&](local_edge_idx_type eid) {
      std::visit(
        [&](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            // do nothing
          } else {
            pl_set_edge_field(name_idx, eid, v);
          }
        },
        val);
    };
    priv_for_all_edges(wrapper, where);
  } else if (name.is_node_series()) {
    auto pnodes_ = m_pnodes;
    bool assigned_ok = true;
    std::visit(
      [&assigned_ok, &name, pnodes_](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // do nothing
        } else if constexpr (std::is_same_v<T, std::string_view>) {
          pnodes_->add_series<std::string_view>(name.unqualified());
        } else if constexpr (std::is_same_v<T, int64_t>) {
          pnodes_->add_series<int64_t>(name.unqualified());
        } else if constexpr (std::is_same_v<T, bool>) {
          pnodes_->add_series<bool>(name.unqualified());
        } else {
          assigned_ok = false;
        }
      },
      val);

    if (!assigned_ok) {
      return std::unexpected("invalid type for value; aborting");
    }
    auto name_idx_o = pl_find_node_series(name);
    if (!name_idx_o.has_value()) {
      return std::unexpected(
        std::format("series {} not found", name.qualified()));
    }
    auto name_idx = name_idx_o.value();

    auto wrapper = [&](local_node_idx_type nid) {
      std::visit(
        [&](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            // Skip monostate
          } else {
            pl_set_node_field(name_idx, nid, v);
          }
        },
        val);
    };
    priv_for_all_nodes(wrapper, where);
  } else {
    return std::unexpected(
      std::format("unknown series name: {}", name.qualified()));
  };

  return to_return;
}
}  // namespace metalldata
