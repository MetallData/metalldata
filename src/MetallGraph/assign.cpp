#include <metall_graph.hpp>

// using data_types =
//   std::variant<size_t, double, bool, std::string, std::monostate>;

namespace metalldata {

metall_graph::return_code metall_graph::assign(
  std::string_view series_name, const metall_graph::data_types& val,
  const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;
  return_code to_return;

  if (!has_series(series_name)) {
    to_return.error = std::format("Series {} not found", series_name);
    return to_return;
  }

  if (is_edge_selector(series_name)) {
    auto pedges_ = m_pedges;
    auto wrapper = [&val, &pedges_, &series_name](record_id_type record_id) {
      std::visit(
        [&pedges_, &series_name, record_id](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            // Skip monostate
          } else if constexpr (std::is_same_v<T, std::string>) {
            pedges_->set(series_name, record_id, std::string_view(v));
          } else if constexpr (std::is_same_v<T, size_t>) {
            pedges_->set(series_name, record_id, static_cast<uint64_t>(v));
          } else {
            pedges_->set(series_name, record_id, v);
          }
        },
        val);
    };
    for_all_edges(wrapper, where);
  } else {
    auto pnodes_ = m_pnodes;
    auto wrapper = [&val, &pnodes_, &series_name](record_id_type record_id) {
      std::visit(
        [&pnodes_, &series_name, record_id](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            // Skip monostate
          } else if constexpr (std::is_same_v<T, std::string>) {
            pnodes_->set(series_name, record_id, std::string_view(v));
          } else if constexpr (std::is_same_v<T, size_t>) {
            pnodes_->set(series_name, record_id, static_cast<uint64_t>(v));
          } else {
            pnodes_->set(series_name, record_id, v);
          }
        },
        val);
    };
    for_all_nodes(wrapper, where);
  };

  return to_return;
}
}  // namespace metalldata