#include <metalldata/metall_graph.hpp>
#include <string_table/string_store.hpp>
#include <ygm/detail/collective.hpp>
#include <ygm/utility/assert.hpp>
#include "metalldata/detail/generic_locator.hpp"

namespace metalldata {

void metall_graph::pasync_insert_node(std::string_view nlbv) {
  // 1. Check if we already have the node in our reverse index. If so, do
  // nothing.
  auto nlb_sa = compact_string::add_string(nlbv, *m_pstring_store);
  if (m_pnode_to_locator->contains(nlb_sa)) {
    return;
  }

  // 2. If not, send async message to owner of the node to insert it.
  detail::rank_type owner = m_partitioner.owner(nlbv);
  auto request = [](ygm_ptr_type pthis, detail::rank_type requester,
                    const std::string& nlb) {
    YGM_ASSERT_RELEASE(pthis->m_partitioner.owner(nlb) == pthis->m_comm.rank());
    auto nloc_o = pthis->pl_get_node_locator(nlb);
    if (!nloc_o.has_value()) {
      auto nid = local_node_idx_type{pthis->m_pnodes->add_record()};
      pthis->pl_set_node_field(pthis->m_node_col_idx, nid,
                               std::string_view{nlb});
      auto lb_sa = compact_string::add_string(nlb, *(pthis->m_pstring_store));
      nloc_o = make_node_locator(pthis->m_comm.rank(), nid);
      pthis->m_pnode_to_locator->insert_or_assign(lb_sa, nloc_o.value());
    }

    auto response = [](ygm_ptr_type pthis, const std::string& nlb,
                       node_locator nl) {
      auto nlb_sa = compact_string::add_string(nlb, *(pthis->m_pstring_store));
      pthis->m_pnode_to_locator->insert_or_assign(nlb_sa, nl);
    };

    // 3. Send response back to requester so they can update their reverse
    // index.
    pthis->m_comm.async(requester, response, pthis, nlb, nloc_o.value());
  };
  m_comm.async(owner, request, pthis, m_comm.rank(), std::string{nlbv});
}

std::optional<metall_graph::local_node_idx_type> metall_graph::pl_get_node_id(
  std::string_view label) const {
  auto nloc_o = pl_get_node_locator(label);
  if (nloc_o.has_value()) {
    if (is_local(nloc_o.value())) {
      return local(nloc_o.value());
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

result<> metall_graph::priv_check_index_integrity() const {
  result<> to_return;
  //
  // Loop over local nodes and check m_pnode_to_idx
  priv_for_all_nodes([&](local_node_idx_type nid) {
    auto nlb = pl_get_node_label(nid);
    auto nid_o = pl_get_node_id(nlb);
    YGM_ASSERT_DEBUG(nid_o.has_value());
    if (nid != nid_o.value()) {
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
    auto uloc_o = pl_get_node_locator(ulb);
    if (!uloc_o.has_value()) {
      to_return.add_warning();
      return;
    }
    auto vloc_o = pl_get_node_locator(vlb);
    if (!vloc_o.has_value()) {
      to_return.add_warning();
      return;
    }

    int u_owner = m_partitioner.owner(ulb);
    if (u_owner != owner(uloc_o.value())) {
      to_return.add_warning();
      return;
    }
    int v_owner = m_partitioner.owner(vlb);
    if (v_owner != owner(vloc_o.value())) {
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
    m_comm.async(u_owner, index_check, std::string(ulb), local(uloc_o.value()));
    m_comm.async(v_owner, index_check, std::string(vlb), local(vloc_o.value()));
  });

  bool local_errors = !to_return.warnings().empty();
  bool global_errors = ygm::logical_or(local_errors, m_comm);
  if (global_errors) {
    to_return = std::unexpected("Index errors found, see warnings for details");
  }

  return to_return;
}

}  // namespace metalldata