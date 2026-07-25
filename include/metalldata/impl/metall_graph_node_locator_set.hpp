// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/container/set.hpp>

namespace metalldata {
/**
 * @brief node_locator_set is specialized for node_locators by deriving from
 * ygm::container::set Provides for_all_local that paritions locators by their
 * owner before executing user Fn.
 *
 */
class metall_graph::node_locator_set
    : public ygm::container::set<node_locator> {
 private:
  using base = ygm::container::set<node_locator>;

 public:
  using base::base;

  /**
   * @brief Collective for_all that executes func over only local nodes
   *
   * @tparam Fn
   * @param func
   */
  template <typename Fn>
  void for_all_local(Fn func) {
    // setup for collective
    static Fn* spfunc = nullptr;
    spfunc = &func;
    static ygm::comm* spcomm = nullptr;
    spcomm = &(comm());
    comm().cf_barrier();
    for_all([](node_locator nl) {
      int  dest = owner(nl);
      auto rlambda = [](local_node_idx_type nid) { (*spfunc)(nid); };
      spcomm->async(dest, rlambda, local(nl));
    });
    comm().barrier();
    spfunc = nullptr;
    spcomm = nullptr;
  }
};

}  // namespace metalldata