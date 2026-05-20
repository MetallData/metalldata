// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

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
    m_pnode_to_idx = manager.construct<local_vertex_map_type>("nodeindex")(
      manager.get_allocator());

    // add the default series for the indices.
    add_series<std::string_view>(NODE_COL);
    add_series<std::string_view>(U_COL);
    add_series<std::string_view>(V_COL);
    add_series<bool>(DIR_COL);

  } else {  // open existing
    comm.barrier();
    m_pmetall_mpi = new metall::utility::metall_mpi_adaptor(
      metall::open_only, m_metall_path, m_comm.get_mpi_comm());
    auto& manager = m_pmetall_mpi->get_local_manager();

    m_pstring_store =
      manager.find<string_store_type>(metall::unique_instance).first;
    m_pnodes       = manager.find<record_store_type>("nodes").first;
    m_pedges       = manager.find<record_store_type>("edges").first;
    m_pnode_to_idx = manager.find<local_vertex_map_type>("nodeindex").first;

    if (!m_pnodes || !m_pedges) {
      m_comm.cerr0(
        "Error: Failed to find required data structures in metall store");
      delete m_pmetall_mpi;
      m_pmetall_mpi   = nullptr;
      m_pstring_store = nullptr;
      m_pnodes        = nullptr;
      m_pedges        = nullptr;
      m_pnode_to_idx  = nullptr;
    }
  }

  ///\todo Instead of hard crashing, need a nicer fail, maybe .good() method
  YGM_ASSERT_RELEASE(has_node_series(NODE_COL));
  YGM_ASSERT_RELEASE(has_edge_series(U_COL));
  YGM_ASSERT_RELEASE(has_edge_series(V_COL));
  YGM_ASSERT_RELEASE(has_edge_series(DIR_COL));

  //
  // Find required column names
  m_u_col_idx    = m_pedges->find_series(U_COL.unqualified());
  m_v_col_idx    = m_pedges->find_series(V_COL.unqualified());
  m_dir_col_idx  = m_pedges->find_series(DIR_COL.unqualified());
  m_node_col_idx = m_pnodes->find_series(NODE_COL.unqualified());
}

metall_graph::~metall_graph() {
  // Ensure all processors are together in the destructor
  m_comm.barrier();

  // We don't free these because they are persistent in the metall store
  m_pstring_store = nullptr;
  m_pnodes        = nullptr;
  m_pedges        = nullptr;
  m_pnode_to_idx  = nullptr;

  // Destroy the metall manager
  delete m_pmetall_mpi;
  m_pmetall_mpi = nullptr;
}

// drop_series requires a qualified selector name (starts with node. or edge.)
bool metall_graph::drop_series(const series_name& name) {
  if (RESERVED_COLUMN_NAMES.contains(name)) {
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

metall_graph::return_code metall_graph::rename_series(
  const series_name& old_name, const series_name& new_name) {
  metall_graph::return_code to_return;
  if (RESERVED_COLUMN_NAMES.contains(old_name)) {
    to_return.error =
      std::format("Cannot rename reserved column {}", old_name.qualified());
    return to_return;
  }

  if (RESERVED_COLUMN_NAMES.contains(new_name)) {
    to_return.error =
      std::format("{} is a reserved name; cannot rename", new_name.qualified());
    return to_return;
  }

  if (old_name.prefix() != new_name.prefix()) {
    to_return.error =
      std::format("Series must be of the same type (got {} and {}) ",
                  old_name.prefix(), new_name.prefix());
    return to_return;
  }

  if (old_name.is_node_series()) {
    m_pnodes->rename_series(old_name.unqualified(), new_name.unqualified());
    return to_return;
  }
  if (old_name.is_edge_series()) {
    m_pedges->rename_series(old_name.unqualified(), new_name.unqualified());
    return to_return;
  }
  to_return.error =
    std::format("Unknown series type: {}", old_name.qualified());
  return to_return;
}

/// Converts a multiseries series_type variant to a metall_graph data_types
/// variant. string_view is promoted to string (owning). int64_t and uint64_t
/// are cast to size_t — int64_t conversion is lossy for negative values.
metall_graph::data_types metall_graph::priv_series_to_data_type(
  const record_store_type::series_type& sv) {
  return std::visit(
    [](const auto& val) -> metall_graph::data_types {
      using T = std::decay_t<decltype(val)>;
      if constexpr (std::is_same_v<T, uint64_t>)
        return static_cast<size_t>(val);
      else if constexpr (std::is_same_v<T, int64_t>)
        return static_cast<size_t>(val);  // lossy if negative
      else if constexpr (std::is_same_v<T, std::string_view>)
        return std::string(val);
      else
        return val;  // bool, double, monostate
    },
    sv);
}

}  // namespace metalldata
