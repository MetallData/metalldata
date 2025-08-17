// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <string>
#include <vector>
#include <set>
#include <map>
#include <string_view>
#include <filesystem>
#include <cassert>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>

#include "metall_graph.hpp"

namespace metalldata {

metall_graph::metall_graph(ygm::comm& comm, std::string_view path,
                           bool overwrite)
    : m_comm(comm), m_metall_path(path) {
  //
  // Check if metall store already exists and overwrite if requested
  bool path_exists = std::filesystem::exists(path);
  if (path_exists) {
    if (overwrite) {
      std::filesystem::remove_all(path);
    }
    comm.barrier();
    m_pmetall_mpi = new metall::utility::metall_mpi_adaptor(
        metall::create_only, m_metall_path, m_comm.get_mpi_comm());
    auto& manager = m_pmetall_mpi->get_local_manager();

    auto* string_store = manager.construct<string_store_type>(
        metall::unique_instance)(manager.get_allocator());
    m_pnodes = manager.construct<record_store_type>("nodes")(
        string_store, manager.get_allocator());
    m_pdirected_edges = manager.construct<record_store_type>("dedges")(
        string_store, manager.get_allocator());
    m_pundirected_edges = manager.construct<record_store_type>("uedges")(
        string_store, manager.get_allocator());
  } else {  // open existing
    comm.barrier();
    m_pmetall_mpi = new metall::utility::metall_mpi_adaptor(
        metall::create_only, m_metall_path, m_comm.get_mpi_comm());
    auto& manager = m_pmetall_mpi->get_local_manager();

    m_pnodes            = manager.find<record_store_type>("nodes").first;
    m_pdirected_edges   = manager.find<record_store_type>("dedges").first;
    m_pundirected_edges = manager.find<record_store_type>("uedges").first;
  }

  //
  // Open metall store
}

metall_graph::~metall_graph() {
  // Ensure all processors are together in the destructor
  m_comm.barrier();

  // We don't free these because they are persistent in the metall store
  m_pnodes            = nullptr;
  m_pdirected_edges   = nullptr;
  m_pundirected_edges = nullptr;

  // Destroy the metall manager
  delete m_pmetall_mpi;
  m_pmetall_mpi = nullptr;
}

template <typename T>
bool metall_graph::add_series(std::string_view name) {
  if (name.find("node.") == 0) {
    if (m_pnodes->contains_series(name)) {
      return false;
    }
    m_comm.cout0("Adding Series: ", name);
    m_pnodes->add_series<T>(name);
  } else if (name.find("edge.") == 0) {
    if (m_pdirected_edges->contains_series(name)) {
      return false;
    }
    m_comm.cout0("Adding Series: ", name);
    m_pdirected_edges->add_series<T>(name);
    m_pundirected_edges->add_series<T>(name);
  }
  return false;
}

metall_graph::return_code metall_graph::ingest_parquet_edges(
    std::string_view path, bool recursive, std::string_view col_u,
    std::string_view col_v, bool directed,
    const std::vector<std::string>& meta) {
  return_code to_return;

  //
  // Setup parquet reader
  std::vector<std::string> paths;
  paths.push_back(path.data());
  ygm::io::parquet_parser parquetp(m_comm, paths, recursive);
  const auto&             schema = parquetp.get_schema();

  int                        ucol = -1;
  int                        vcol = -1;
  std::set<std::string>      metaset(meta.begin(), meta.end());
  std::map<int, std::string> metacols;

  for (size_t i = 0; i < schema.size(); ++i) {
    if (schema[i].name == col_u) {
      YGM_ASSERT_RELEASE(ucol == -1);
      YGM_ASSERT_RELEASE(schema[i].type.equal(parquet::Type::BYTE_ARRAY));
      ucol = i;
      continue;
    }
    if (schema[i].name == col_v) {
      YGM_ASSERT_RELEASE(vcol == -1);
      YGM_ASSERT_RELEASE(schema[i].type.equal(parquet::Type::BYTE_ARRAY));
      vcol = i;
      continue;
    }
    if (metaset.contains((schema[i].name))) {
      std::string prefix("edge.");
      if (schema[i].type.equal(parquet::Type::INT32) ||
          schema[i].type.equal(parquet::Type::INT64)) {
        if (add_series<int64_t>(prefix + schema[i].name)) {
          metacols[i] = prefix + schema[i].name;
        } else {
          m_comm.cerr0("Warning: column already exists: ",
                       prefix + schema[i].name);
        }
      } else if (schema[i].type.equal(parquet::Type::FLOAT) ||
                 schema[i].type.equal(parquet::Type::DOUBLE)) {
        if (add_series<double>(prefix + schema[i].name)) {
          metacols[i] = prefix + schema[i].name;
        } else {
          m_comm.cerr0("Warning: column already exists: ",
                       prefix + schema[i].name);
        }
      } else if (schema[i].type.equal(parquet::Type::BYTE_ARRAY)) {
        if (add_series<std::string_view>(prefix + schema[i].name)) {
          metacols[i] = prefix + schema[i].name;
        } else {
          m_comm.cerr0("Warning: column already exists: ",
                       prefix + schema[i].name);
        }
      } else {
        std::stringstream ss;
        ss << "Unsupported column type: " << schema[i].type;
        to_return.warnings[ss.str()]++;
      }
    }
  }

  //   for (const auto& s : schema) {
  //     if (s.name == col_u) {
  //     }

  //     if (s.type.equal(parquet::Type::INT32) ||
  //         s.type.equal(parquet::Type::INT64)) {
  //       // auto series_index = record_store->add_series<int64_t>(s.name);
  //       vec_col_ids.push_back(series_index);
  //     } else if (s.type.equal(parquet::Type::FLOAT) or
  //                s.type.equal(parquet::Type::DOUBLE)) {
  //       auto series_index = record_store->add_series<double>(s.name);
  //       vec_col_ids.push_back(series_index);
  //     } else if (s.type.equal(parquet::Type::BYTE_ARRAY)) {
  //       auto series_index =
  //       record_store->add_series<std::string_view>(s.name);
  //       vec_col_ids.push_back(series_index);
  //     } else {
  //       comm.cerr0() << "Unsupported column type: " << s.type << std::endl;
  //       MPI_Abort(comm.get_mpi_comm(), EXIT_FAILURE);
  //     }
  //   }

  return to_return;
}

}  // namespace metalldata