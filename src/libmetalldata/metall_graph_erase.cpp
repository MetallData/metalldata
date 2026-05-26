#include <metalldata/metall_graph.hpp>
#include "ygm/utility/assert.hpp"

namespace metalldata {
metall_graph::return_code metall_graph::erase_edges(const where_clause &where) {
  metall_graph::return_code to_return;

  priv_for_all_edges([&](auto rid) { m_pedges->remove_record(rid); }, where);

  return to_return;
}

metall_graph::return_code metall_graph::erase_edges(
  const metall_graph::series_name       &name,
  boost::unordered_flat_set<std::string> haystack) {
  metall_graph::return_code to_return;

  auto idx_o = m_pedges->find_series(name.unqualified());
  if (!idx_o.has_value()) {
    to_return.error = std::format("Series {} not found", name.unqualified());
    return to_return;
  }

  auto idx = idx_o.value();

  priv_for_all_edges([&](auto rid) {
    auto val_opt = m_pedges->get<std::string_view>(idx, rid);
    YGM_ASSERT_RELEASE(val_opt.has_value());
    if (haystack.contains(std::string(val_opt.value()))) {
      m_pedges->remove_record(rid);
    }
  });

  return to_return;
}

}  // namespace metalldata