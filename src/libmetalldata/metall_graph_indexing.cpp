#include <metalldata/metall_graph.hpp>
#include <string_table/string_store.hpp>
#include <ygm/detail/collective.hpp>
#include <ygm/utility/assert.hpp>

namespace metalldata {

metall_graph::local_node_idx_type metall_graph::pl_node_find_or_insert(
  std::string_view label) {
  YGM_ASSERT_RELEASE(m_partitioner.owner(label) == m_comm.rank());
  auto label_sa = compact_string::add_string(label, *m_pstring_store);
  if (!m_pnode_to_idx->contains(label_sa)) {
    auto nid = local_node_idx_type{m_pnodes->add_record()};
    pl_set_node_field(m_node_col_idx, nid, label);
    m_pnode_to_idx->insert_or_assign(label_sa, nid);
    return nid;
  }
  return m_pnode_to_idx->at(label_sa);
}

// TODO unify with pl_get_node_locator
std::optional<metall_graph::local_node_idx_type> metall_graph::pl_get_node_id(
  std::string_view label) const {
  YGM_ASSERT_RELEASE(m_partitioner.owner(label) == m_comm.rank());
  auto label_osa = compact_string::find_string(label, *m_pstring_store);
  if (label_osa.has_value()) {
    if (m_pnode_to_idx->contains(label_osa.value())) {
      return m_pnode_to_idx->at(label_osa.value());
    }
  }
  return std::nullopt;
}

std::optional<metall_graph::node_locator> metall_graph::pl_get_node_locator(
  std::string_view label) const {
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
  static metall_graph* spthis = nullptr;
  spthis = this;
  m_comm.barrier();

  // Index local nodes.
  priv_for_all_nodes([&](local_node_idx_type nid) {
    auto nlb = pl_get_node_label(nid);
    auto u_sa = compact_string::add_string(nlb, *m_pstring_store);
    auto nl = make_node_locator(m_comm.rank(), nid);
    m_pnode_to_locator->insert_or_assign(u_sa, nl);
  });

  // Index edges.   Query request and response necessary.
  priv_for_all_edges([&](local_edge_idx_type eid) {
    auto [ulb, vlb] = pl_get_edge_uv_labels(eid);
    auto u_sa = compact_string::add_string(ulb, *m_pstring_store);
    auto v_sa = compact_string::add_string(vlb, *m_pstring_store);

    auto request = [](int requester, const std::string& label) {
      auto nid_o = spthis->pl_get_node_id(label);
      YGM_ASSERT_RELEASE(nid_o.has_value());
      auto nl = make_node_locator(spthis->m_comm.rank(), nid_o.value());

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
  m_comm.barrier();
}

result<> metall_graph::priv_check_index_integrity() const {
  result<> to_return;
  //
  // Loop over local nodes and check m_pnode_to_idx
  priv_for_all_nodes([&](local_node_idx_type nid) {
    auto nlb = pl_get_node_label(nid);
    auto nido = pl_get_node_id(nlb);
    YGM_ASSERT_DEBUG(nido.has_value());
    if (nid != nido.value()) {
      to_return.add_warning();
    }
  });

  //
  // Loop over local edges and check m_pnode_to_locator by sending message to
  // node owner
  static const metall_graph* spthis = nullptr;
  spthis = this;
  static result<>* spto_return = nullptr;
  spto_return = &to_return;
  m_comm.barrier();
  priv_for_all_edges([&](local_edge_idx_type eid) {
    auto [ulb, vlb] = pl_get_edge_uv_labels(eid);
    auto uloco = pl_get_node_locator(ulb);
    if (!uloco.has_value()) {
      to_return.add_warning();
      return;
    }
    auto vloco = pl_get_node_locator(vlb);
    if (!vloco.has_value()) {
      to_return.add_warning();
      return;
    }

    int u_owner = m_partitioner.owner(ulb);
    if (u_owner != owner(uloco.value())) {
      to_return.add_warning();
      return;
    }
    int v_owner = m_partitioner.owner(vlb);
    if (v_owner != owner(vloco.value())) {
      to_return.add_warning();
      return;
    }

    auto index_check = [](const std::string& label, local_node_idx_type nid) {
      auto nlb = spthis->pl_get_node_label(nid);
      if (label != nlb) {
        spto_return->add_warning();
        return;
      }
    };
    m_comm.async(u_owner, index_check, std::string(ulb), local(uloco.value()));
    m_comm.async(v_owner, index_check, std::string(vlb), local(vloco.value()));
  });

  bool local_errors = !to_return.warnings().empty();
  bool global_errors = ygm::logical_or(local_errors, m_comm);
  if (global_errors) {
    to_return = std::unexpected("Index errors found, see warnings for details");
  }

  return to_return;
}

}  // namespace metalldata