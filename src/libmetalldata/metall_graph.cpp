// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <stdexcept>
#include <string>
#include <variant>
#include <vector>
#include <set>
#include <map>
#include <string_view>
#include <filesystem>
#include <cassert>
#include <cstdint>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>

#include <metalldata/metall_graph.hpp>
// #include <metall_jl/metall_jl.hpp>
#include <fcntl.h>

#include <boost/graph/graph_traits.hpp>
#include <multiseries/multiseries_record.hpp>
#include <ygm/container/set.hpp>
#include <ygm/container/counting_set.hpp>
#include "metall/tags.hpp"
#include "ygm/utility/assert.hpp"

namespace metalldata {

metall_graph::metall_graph(ygm::comm& comm, std::string_view path,
                           bool overwrite)
    : m_comm(comm), m_metall_path(path), m_partitioner(m_comm) {
  //
  // Check if metall store already exists and overwrite if requested
  // There are three states:
  // path does not exist: create new, open RW
  // overwrite: remove, then create new, open RW
  // path exists: open RW

  bool path_exists = std::filesystem::exists(path);
  if (!path_exists || overwrite) {
    if (overwrite) {
      std::filesystem::remove_all(path);
    }
    comm.barrier();
    m_pmetall_mpi = new metall::utility::metall_mpi_adaptor(
      metall::create_only, m_metall_path, m_comm.get_mpi_comm());
    auto& manager = m_pmetall_mpi->get_local_manager();

    m_pstring_store = manager.construct<string_store_type>(
      metall::unique_instance)(manager.get_allocator());
    m_pnodes = manager.construct<record_store_type>("nodes")(
      m_pstring_store, manager.get_allocator());
    m_pedges = manager.construct<record_store_type>("edges")(
      m_pstring_store, manager.get_allocator());
    m_pnode_to_idx = manager.construct<map_local_node_to_local_id_type>(
      "localnodeindex")(manager.get_allocator());
    m_pnode_to_locator = manager.construct<map_node_to_locator_type>(
      "globalnodeindex")(manager.get_allocator());

    // add the default series for the indices.
    add_series<std::string_view>(series_name::NODE_COL);
    add_series<std::string_view>(series_name::U_COL);
    add_series<std::string_view>(series_name::V_COL);
    add_series<bool>(series_name::DIR_COL);

  } else {  // open existing
    comm.barrier();
    m_pmetall_mpi = new metall::utility::metall_mpi_adaptor(
      metall::open_only, m_metall_path, m_comm.get_mpi_comm());
    auto& manager = m_pmetall_mpi->get_local_manager();

    m_pstring_store =
      manager.find<string_store_type>(metall::unique_instance).first;
    m_pnodes = manager.find<record_store_type>("nodes").first;
    m_pedges = manager.find<record_store_type>("edges").first;
    m_pnode_to_idx =
      manager.find<map_local_node_to_local_id_type>("localnodeindex").first;
    m_pnode_to_locator =
      manager.find<map_node_to_locator_type>("globalnodeindex").first;

    if (!m_pnodes || !m_pedges) {
      m_comm.cerr0(
        "Error: Failed to find required data structures in metall store");
      delete m_pmetall_mpi;
      m_pmetall_mpi = nullptr;
      m_pstring_store = nullptr;
      m_pnodes = nullptr;
      m_pedges = nullptr;
      m_pnode_to_idx = nullptr;
    }
  }

  ///\todo Instead of hard crashing, need a nicer fail, maybe .good() method
  YGM_ASSERT_RELEASE(has_node_series(series_name::NODE_COL));
  YGM_ASSERT_RELEASE(has_edge_series(series_name::U_COL));
  YGM_ASSERT_RELEASE(has_edge_series(series_name::V_COL));
  YGM_ASSERT_RELEASE(has_edge_series(series_name::DIR_COL));

  //
  // Find required column names
  auto u_col_idx_o = m_pedges->find_series(series_name::U_COL.unqualified());
  auto v_col_idx_o = m_pedges->find_series(series_name::V_COL.unqualified());
  auto dir_col_idx_o =
    m_pedges->find_series(series_name::DIR_COL.unqualified());
  auto node_col_idx_o =
    m_pnodes->find_series(series_name::NODE_COL.unqualified());
  YGM_ASSERT_RELEASE(u_col_idx_o.has_value());
  YGM_ASSERT_RELEASE(v_col_idx_o.has_value());
  YGM_ASSERT_RELEASE(dir_col_idx_o.has_value());
  YGM_ASSERT_RELEASE(node_col_idx_o.has_value());

  m_u_col_idx = edge_series_idx_type{u_col_idx_o.value()};
  m_v_col_idx = edge_series_idx_type{v_col_idx_o.value()};
  m_dir_col_idx = edge_series_idx_type{dir_col_idx_o.value()};
  m_node_col_idx = node_series_idx_type{node_col_idx_o.value()};
}

metall_graph::~metall_graph() {
  // Ensure all processors are together in the destructor
  m_comm.barrier();

  // We don't free these because they are persistent in the metall store
  m_pstring_store = nullptr;
  m_pnodes = nullptr;
  m_pedges = nullptr;
  m_pnode_to_idx = nullptr;

  // Destroy the metall manager
  delete m_pmetall_mpi;
  m_pmetall_mpi = nullptr;
}

// drop_series requires a qualified selector name (starts with node. or edge.)
bool metall_graph::drop_series(const series_name& name) {
  if (name.is_reserved()) {
    m_comm.cerr0("Cannot remove reserved column ", name.qualified());
    return false;
  }
  if (name.is_node_series()) {
    return m_pnodes->remove_series(name.unqualified());
  }
  if (name.is_edge_series()) {
    return m_pedges->remove_series(name.unqualified());
  }
  m_comm.cerr0("Unknown series name", name.qualified());
  return false;
}

result<> metall_graph::rename_series(const series_name& old_name,
                                     const series_name& new_name) {
  if (old_name.is_reserved()) {
    return std::unexpected(
      std::format("cannot rename reserved column {}", old_name.qualified()));
  }

  if (new_name.is_reserved()) {
    return std::unexpected(std::format("{} is a reserved name; cannot rename",
                                       new_name.qualified()));
  }

  if (old_name.prefix() != new_name.prefix()) {
    return std::unexpected(
      std::format("series must be of the same type (got {} and {}) ",
                  old_name.prefix(), new_name.prefix()));
  }

  if (old_name.is_node_series()) {
    m_pnodes->rename_series(old_name.unqualified(), new_name.unqualified());
    return result<>{};
  }
  if (old_name.is_edge_series()) {
    m_pedges->rename_series(old_name.unqualified(), new_name.unqualified());
    return result<>{};
  }
  return std::unexpected(
    std::format("Unknown series type: {}", old_name.qualified()));
}

/// Converts a multiseries series_type variant to a metall_graph data_types
/// variant. string_view is promoted to string (owning). int64_t and uint64_t
/// are cast to int64_t — uint64_t will now throw runtime.
metall_graph::count_types metall_graph::priv_series_to_count_type(
  const record_store_type::series_type& sv) {
  return std::visit(
    [](const auto& val) -> metall_graph::count_types {
      using T = std::decay_t<decltype(val)>;
      if constexpr (std::is_same_v<T, uint64_t>)
        throw std::runtime_error("uint64_t is not supported.");
      else if constexpr (std::is_same_v<T, std::string_view>)
        return std::string(val);
      else
        return val;  // bool, double, monostate
    },
    sv);
}

size_t metall_graph::num_edges(const metall_graph::where_clause& where) const {
  size_t local_size = pl_num_edges();
  if (!where.empty()) {
    local_size = 0;
    priv_for_all_edges([&](auto) { ++local_size; }, where);
  }
  return ygm::sum(local_size, m_comm);
}

size_t metall_graph::num_nodes(const metall_graph::where_clause& where) const {
  size_t local_size = pl_num_nodes();
  if (!where.empty()) {
    local_size = 0;
    priv_for_all_nodes([&](auto) { ++local_size; }, where);
  }
  return ygm::sum(local_size, m_comm);
}

}  // namespace metalldata
