#include <metalldata/metall_graph.hpp>
#include <string_table/string_store.hpp>

namespace metalldata {

metall_graph::local_node_idx_type metall_graph::priv_local_node_find_or_insert(
  std::string_view label) {
  YGM_ASSERT_RELEASE(m_partitioner.owner(label) == m_comm.rank());
  auto v_in_ss = compact_string::add_string(label, *m_pstring_store);
  if (!m_pnode_to_idx->contains(v_in_ss)) {
    auto nid = local_node_idx_type{m_pnodes->add_record()};
    priv_local_set_node_field(m_node_col_idx, nid, label);
    m_pnode_to_idx->insert_or_assign(v_in_ss, nid);
    return nid;
  }
  return m_pnode_to_idx->at(v_in_ss);
}

std::optional<metall_graph::local_node_idx_type>
metall_graph::priv_local_node_find(std::string_view label) const {
  YGM_ASSERT_RELEASE(m_partitioner.owner(label) == m_comm.rank());
  auto id_osa = compact_string::find_string(label, *m_pstring_store);
  if (id_osa.has_value()) {
    if (m_pnode_to_idx->contains(id_osa.value())) {
      return m_pnode_to_idx->at(id_osa.value());
    }
  }
  return std::nullopt;
}

void metall_graph::priv_update_reverse_node_index() {
  // setup for collective
  static metall_graph* spthis = this;
  m_comm.barrier();

  priv_for_all_edges([&](local_edge_idx_type eid) {
    auto uv_o = priv_local_get_edge_uv_labels(eid);
    YGM_ASSERT_RELEASE(uv_o.has_value());
    auto u_sa =
      compact_string::add_string(uv_o.value().first, *m_pstring_store);
    auto v_sa =
      compact_string::add_string(uv_o.value().second, *m_pstring_store);

    auto request = [](int requester, const std::string& label) {
      auto nid_o = spthis->priv_local_node_find(label);
      YGM_ASSERT_RELEASE(nid_o.has_value());
      node_locator nl(spthis->m_comm.rank(), nid_o.value());

      auto response = [nl](const std::string& label) {
        auto l_sa =
          compact_string::add_string(label, *(spthis->m_pstring_store));
        if (!spthis->m_pnode_to_locator->contains(l_sa)) {
          spthis->m_pnode_to_locator->insert_or_assign(l_sa, nl);
        }
      };
      spthis->m_comm.async(requester, response, label);
    };

    if (!m_pnode_to_locator->contains(u_sa)) {
      m_comm.async(m_partitioner.owner(u_sa.to_view()), request, m_comm.rank(),
                   std::string{u_sa.to_view()});
    }
    if (!m_pnode_to_locator->contains(v_sa)) {
      m_comm.async(m_partitioner.owner(v_sa.to_view()), request, m_comm.rank(),
                   std::string{v_sa.to_view()});
    }
  });
}

}  // namespace metalldata