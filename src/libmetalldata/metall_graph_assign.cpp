
#include <metalldata/metall_graph.hpp>

namespace metalldata {

metall_graph::return_code metall_graph::assign(
  series_name name, const metall_graph::data_types& val,
  const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;
  return_code to_return;

  if (has_series(name)) {
    to_return.error = std::format("Series {} already exists", name.qualified());
    return to_return;
  }

  if (name.is_edge_series()) {
    auto pedges_     = m_pedges;
    bool assigned_ok = true;
    std::visit(
      [&assigned_ok, &name, pedges_](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // do nothing
        } else if constexpr (std::is_same_v<T, std::string> ||
                             std::is_same_v<T, std::string_view>) {
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
      to_return.error = "Invalid type for value; aborting";
      return to_return;
    }
    auto name_idx_o = m_pedges->find_series(name.unqualified());
    if (!name_idx_o.has_value()) {
      to_return.error = std::format("Series {} not found", name.qualified());
      return to_return;
    }
    auto name_idx = name_idx_o.value();
    auto wrapper = [&val, pedges_, name_idx](record_id_type record_id) {
      std::visit(
        [pedges_, name_idx, record_id](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            // Skip monostate
          } else if constexpr (std::is_same_v<T, std::string>) {
            pedges_->set(name_idx, record_id, std::string_view(v));
          } else {
            pedges_->set(name_idx, record_id, v);
          }
        },
        val);
    };
    priv_for_all_edges(wrapper, where);
  } else if (name.is_node_series()) {
    auto pnodes_     = m_pnodes;
    bool assigned_ok = true;
    std::visit(
      [&assigned_ok, &name, pnodes_](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // do nothing
        } else if constexpr (std::is_same_v<T, std::string> ||
                             std::is_same_v<T, std::string_view>) {
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
      to_return.error = "Invalid type for value; aborting";
      return to_return;
    }
    auto name_idx_o = m_pnodes->find_series(name.unqualified());
    if (!name_idx_o.has_value()) {
      to_return.error = std::format("Series {} not found", name.qualified());
      return to_return;
    }
    auto name_idx = name_idx_o.value();

    auto wrapper = [&val, pnodes_, name_idx](record_id_type record_id) {
      std::visit(
        [pnodes_, name_idx, record_id](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            // Skip monostate
          } else if constexpr (std::is_same_v<T, std::string>) {
            pnodes_->set(name_idx, record_id, std::string_view(v));
          } else {
            pnodes_->set(name_idx, record_id, v);
          }
        },
        val);
    };
    priv_for_all_nodes(wrapper, where);
  } else {
    to_return.error = std::format("Unknown series name: {}", name.qualified());
  };

  return to_return;
}
}  // namespace metalldata