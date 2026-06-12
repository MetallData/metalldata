#include <metalldata/metall_graph.hpp>
#include <string_table/string_store.hpp>
#include <ygm/detail/collective.hpp>
#include <ygm/utility/assert.hpp>

namespace metalldata {

metall_graph::local_node_idx_type metall_graph::priv_local_node_find_or_insert(
  std::string_view label) {
  YGM_ASSERT_RELEASE(m_partitioner.owner(label) == m_comm.rank());
  auto label_osa = compact_string::add_string(label, *m_pstring_store);
  if (!m_pnode_to_idx->contains(label_osa)) {
    auto nid = local_node_idx_type{m_pnodes->add_record()};
    priv_local_set_node_field(m_node_col_idx, nid, label);
    m_pnode_to_idx->insert_or_assign(label_osa, nid);
    return nid;
  }
  return m_pnode_to_idx->at(label_osa);
}

std::optional<metall_graph::local_node_idx_type>
metall_graph::priv_local_get_node_id(std::string_view label) const {
  YGM_ASSERT_RELEASE(m_partitioner.owner(label) == m_comm.rank());
  auto label_osa = compact_string::find_string(label, *m_pstring_store);
  if (label_osa.has_value()) {
    if (m_pnode_to_idx->contains(label_osa.value())) {
      return m_pnode_to_idx->at(label_osa.value());
    }
  }
  return std::nullopt;
}

std::optional<metall_graph::node_locator>
metall_graph::priv_local_get_node_locator(std::string_view label) const {
  auto label_osa = compact_string::find_string(label, *m_pstring_store);
  if (label_osa.has_value()) {
    if (m_pnode_to_locator->contains(label_osa.value())) {
      return m_pnode_to_locator->at(label_osa.value());
    }
  }
  return std::nullopt;
}

void metall_graph::priv_update_reverse_node_index() {
  // Setup for collective.
  static metall_graph* spthis = this;
  m_comm.barrier();

  // Index local nodes.
  priv_for_all_nodes([&](local_node_idx_type nid) {
    auto u_o = priv_local_get_node_label(nid);
    if (u_o.has_value()) {
      auto u_sa = compact_string::add_string(u_o.value(), *m_pstring_store);
      auto nl_o = init_node_locator(m_comm.rank(), nid);
      YGM_ASSERT_DEBUG(nl_o.has_value());
      m_pnode_to_locator->insert_or_assign(u_sa, nl_o.value());
    }
  });

  // Index edges.   Query request and response necessary.
  priv_for_all_edges([&](local_edge_idx_type eid) {
    auto uv_o = priv_local_get_edge_uv_labels(eid);
    YGM_ASSERT_RELEASE(uv_o.has_value());
    auto u_sa =
      compact_string::add_string(uv_o.value().first, *m_pstring_store);
    auto v_sa =
      compact_string::add_string(uv_o.value().second, *m_pstring_store);

    auto request = [](int requester, const std::string& label) {
      auto nid_o = spthis->priv_local_get_node_id(label);
      YGM_ASSERT_RELEASE(nid_o.has_value());
      auto nl_o = init_node_locator(spthis->m_comm.rank(), nid_o.value());
      YGM_ASSERT_DEBUG(nl_o.has_value());
      node_locator nl = nl_o.value();

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

result<> metall_graph::priv_check_index_integrity() const {
  result<> to_return;
  //
  // Loop over local nodes and check m_pnode_to_idx
  priv_for_all_nodes([&](local_node_idx_type nid) {
    auto u_o = priv_local_get_node_label(nid);
    if (!u_o.has_value()) {
      to_return.add_warning();
      return;
    }
    auto u_id_o = priv_local_get_node_id(u_o.value());
    if (!u_id_o.has_value()) {
      to_return.add_warning();
      return;
    }
    if (u_id_o.value() != nid) {
      to_return.add_warning();
    }
  });

  //
  // Loop over low edges and check m_pnode_to_locator by sending message to node
  // owner
  static const metall_graph* spthis = this;
  static result<>*           spto_return = &to_return;
  m_comm.barrier();
  priv_for_all_edges([&](local_edge_idx_type eid) {
    auto uv_o = priv_local_get_edge_uv_labels(eid);
    if (!uv_o.has_value()) {
      to_return.add_warning();
      return;
    }
    std::string u_label{uv_o.value().first};
    std::string v_label{uv_o.value().second};
    auto        u_locator_o = priv_local_get_node_locator(u_label);
    if (!u_locator_o.has_value()) {
      to_return.add_warning();
      return;
    }

    auto v_locator_o = priv_local_get_node_locator(v_label);
    if (!v_locator_o.has_value()) {
      to_return.add_warning();
      return;
    }

    int u_owner = m_partitioner.owner(u_label);
    if (u_owner != owner(u_locator_o.value())) {
      to_return.add_warning();
      return;
    }
    int v_owner = m_partitioner.owner(v_label);
    if (v_owner != owner(v_locator_o.value())) {
      to_return.add_warning();
      return;
    }

    auto index_check = [](const std::string& label, local_node_idx_type nid) {
      auto nlabel_o = spthis->priv_local_get_node_label(nid);
      if (!nlabel_o.has_value()) {
        spto_return->add_warning();
        return;
      }
      if (label != nlabel_o.value()) {
        spto_return->add_warning();
        return;
      }
    };
    m_comm.async(u_owner, index_check, u_label, local(u_locator_o.value()));
    m_comm.async(v_owner, index_check, v_label, local(v_locator_o.value()));
  });

  bool local_errors = !to_return.warnings().empty();
  bool global_errors = ygm::logical_or(local_errors, m_comm);
  if (global_errors) {
    to_return = std::unexpected("Index errors found, see warnings for details");
  }

  return to_return;
}

}  // namespace metalldata