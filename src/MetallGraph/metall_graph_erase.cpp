#include <metall_graph.hpp>

namespace metalldata {
metall_graph::return_code metall_graph::erase_edges(const where_clause &where) {
  metall_graph::return_code to_return;

  for_all_edges([&](auto rid) { m_pedges->remove_record(rid); }, where);

  return to_return;
}

metall_graph::return_code metall_graph::erase_edges(
  const metall_graph::series_name       &name,
  boost::unordered_flat_set<std::string> haystack) {
  metall_graph::return_code to_return;

  if (!has_edge_series(name)) {
    to_return.error = std::format("Series {} not found", name.unqualified());
    return to_return;
  }

  auto idx = m_pedges->find_series(name.unqualified());

  for_all_edges([&](auto rid) {
    auto val = m_pedges->get<std::string_view>(idx, rid);
    if (haystack.contains(std::string(val))) {
      m_pedges->remove_record(rid);
    }
  });

  return to_return;
}

}  // namespace metalldata