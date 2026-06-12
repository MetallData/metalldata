#pragma once
#include <ygm/container/set.hpp>

namespace metalldata {
class metall_graph::nodeset {
 public:
  template <typename T>
  nodeset(T&& arg) : m_set(std::forward<T>(arg)) {}

  /**
   * @brief Inserts node_locator into set
   *
   * @param nl
   */
  void async_insert(node_locator nl) { m_set.async_insert(nl); }

  template <typename Fn>
  void for_all(Fn func) {
    m_set.for_all(func);
  }

  /**
   * @brief Collective for_all that executes func over only local nodes
   *
   * @tparam Fn
   * @param func
   */
  template <typename Fn>
  void for_all_local(Fn func) {
    // setup for collective
    static Fn*        spfunc = func;
    static ygm::comm* spcomm = m_set.comm();
    m_set.comm().cf_barrier();
    m_set.for_all([](node_locator nl) {
      int  dest = owner(nl);
      auto rlambda = [](local_node_idx_type nid) { (*spfunc)(nid); };
      spcomm->async(dest, rlambda, local(nl));
    });
  }

  /**
   * @brief Collective operation to look up items that exist within set
   *
   * @param values Values local rank wants to look up in set
   * @return `std::set` of provided values that exist within the YGM set
   */
  template <typename STLValueContainer>
  std::set<node_locator> gather_values(const STLValueContainer& values) {
    return m_set.gather_values(values);
  }

 private:
  ygm::container::set<node_locator> m_set;
};
}  // namespace metalldata