#pragma once
#include <metalldata/metall_graph.hpp>
#include <type_traits>
#include <utility>

namespace metalldata {

/**
 * @brief constexprs used to configure bitsets of locators
 *
 */
namespace {
constexpr unsigned RANK_BITS = 20;
constexpr unsigned ID_BITS = 64 - RANK_BITS;
constexpr size_t   MAX_RANK = (size_t{1} << RANK_BITS) - 1;
constexpr size_t   MAX_ID = (size_t{1} << ID_BITS) - 1;
}  // namespace

/**
 * @brief Global node locator that stores both rank owner and local index
 *
 */
struct metall_graph::node_locator {
 public:
  node_locator(int rank, metall_graph::local_node_idx_type nid)
      : m_rank(rank), m_local_id(std::to_underlying(nid)) {}

  int owner() const { return static_cast<int>(m_rank); }

  metall_graph::local_node_idx_type local() const {
    return metall_graph::local_node_idx_type{m_local_id};
  }

  bool good() const { return m_rank != MAX_RANK && m_local_id != MAX_ID; }

 private:
  size_t m_rank : RANK_BITS = MAX_RANK;
  size_t m_local_id : ID_BITS = MAX_ID;
};

/**
 * @brief Global edge locator that stores both rank owner and local index
 *
 */
struct metall_graph::edge_locator {
 public:
  edge_locator(int rank, metall_graph::local_edge_idx_type eid)
      : m_rank(rank), m_local_id(std::to_underlying(eid)) {}

  int owner() const { return static_cast<int>(m_rank); }

  metall_graph::local_edge_idx_type local() const {
    return metall_graph::local_edge_idx_type{m_local_id};
  }

  bool good() const { return m_rank != MAX_RANK && m_local_id != MAX_ID; }

 private:
  size_t m_rank : RANK_BITS = MAX_RANK;
  size_t m_local_id : ID_BITS = MAX_ID;
};

static_assert(sizeof(metall_graph::node_locator) == 8);
static_assert(sizeof(metall_graph::edge_locator) == 8);
static_assert(std::is_standard_layout_v<metall_graph::node_locator>);
static_assert(std::is_standard_layout_v<metall_graph::edge_locator>);

}  // namespace metalldata