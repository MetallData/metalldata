#include <metalldata/metall_graph.hpp>
#include <utility>
#include "ygm/utility/assert.hpp"

namespace metalldata {
result<> metall_graph::erase_edges(const where_clause &where) {
  result<> to_return;

  priv_for_all_edges([&](auto rid) { m_pedges->remove_record(std::to_underlying(rid)); }, where);

  return to_return;
}

result<> metall_graph::erase_edges(
  const metall_graph::series_name       &name,
  boost::unordered_flat_set<std::string> haystack) {
  result<> to_return;

  auto idx_o = priv_local_find_edge_series(name.unqualified());
  if (!idx_o.has_value()) {
    return std::unexpected(
      std::format("series {} not found", name.unqualified()));
  }

  auto idx = idx_o.value();

  priv_for_all_edges([&](auto rid) {
    auto val_o = priv_local_get_edge_field<std::string_view>(idx, rid);
    YGM_ASSERT_RELEASE(val_o.has_value());
    if (haystack.contains(std::string(val_o.value()))) {
      m_pedges->remove_record(std::to_underlying(rid));
    }
  });

  return to_return;
}

}  // namespace metalldata