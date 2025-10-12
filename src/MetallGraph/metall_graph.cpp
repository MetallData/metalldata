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

#include "multiseries/multiseries_record.hpp"

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
  if (name.starts_with("node.")) {
    if (has_node_series(name)) {
      return false;
    }
    m_comm.cout0("Adding Series: ", name);
    m_pnodes->add_series<T>(name);
    return true;
  }
  if (name.starts_with("edge.")) {
    if (has_edge_series(name)) {
      return false;
    }
    m_comm.cout0("Adding Series: ", name);
    m_pdirected_edges->add_series<T>(name);
    m_pundirected_edges->add_series<T>(name);
    return true;
  }
  return false;
}

bool metall_graph::drop_series(std::string_view name) {
  // TODO: does this need to check the prefix?
  if (name == U_COL || name == V_COL) {
    m_comm.cerr0("Cannot remove index series ", name);
    return false;
  }
  if (name.starts_with("node.")) {
    return m_pnodes->remove_series(name);
  }
  bool ures = m_pdirected_edges->remove_series(name);
  return m_pundirected_edges->remove_series(name) && ures;
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
  std::map<std::string, std::string> parquet_to_metall;

  std::vector<std::string> parquet_cols;
  std::vector<std::string> metall_series;
  std::set<std::string>    schemaset;

  if (metaset.size() != meta.size()) {
    m_comm.cerr0("Duplicate parquet columns specified");
    to_return.error = "Duplicate parquet columns specified";
    return to_return;
  }
  for (const auto& el : schema) {
    schemaset.emplace(el.name);
  }

  bool got_u = false;
  bool got_v = false;

  for (size_t i = 0; i < schema.size(); ++i) {
    std::string pcol_name = schema[i].name;
    auto        pcol_type = schema[i].type;
    if (metaset.contains(pcol_name)) {
      metacols[i]             = pcol_name;
      std::string mapped_name = "edge." + pcol_name;
      if (pcol_name == col_u) {
        YGM_ASSERT_RELEASE(pcol_type.equal(parquet::Type::BYTE_ARRAY));

        mapped_name = U_COL;
        got_u       = true;
      } else if (pcol_name == col_v) {
        YGM_ASSERT_RELEASE(pcol_type.equal(parquet::Type::BYTE_ARRAY));
        mapped_name = V_COL;
        got_v       = true;
      }
      parquet_to_metall[pcol_name] = mapped_name;

      std::string col_errs;
      bool        add_series_err;
      if (pcol_type.equal(parquet::Type::INT32) ||
          pcol_type.equal(parquet::Type::INT64)) {
        add_series_err = !add_series<int64_t>(mapped_name);
      } else if (pcol_type.equal(parquet::Type::FLOAT) ||
                 pcol_type.equal(parquet::Type::DOUBLE)) {
        add_series_err = !add_series<double>(mapped_name);
      } else if (pcol_type.equal(parquet::Type::BYTE_ARRAY)) {
        add_series_err = !add_series<std::string>(mapped_name);
      } else {
        std::stringstream ss;
        ss << "Unsupported column type: " << schema[i].type;
        to_return.warnings[ss.str()]++;
      }

      if (add_series_err) {
        to_return.error = "Failed to add source column: " + pcol_name;
      }
    }

    auto metall_edges = m_pdirected_edges;

    // for each row, set the metall data.
    parquetp.for_all(
        meta,
        [&meta, &parquet_to_metall, &metall_edges](
            const std::vector<ygm::io::parquet_parser::parquet_type_variant>&
                row) {
          auto rec = metall_edges->add_record();
          for (size_t i = 0; i < meta.size(); ++i) {
            auto parquet_ser = meta[i];
            auto parquet_val = row[i];
            if (!std::holds_alternative<std::monostate>(parquet_val)) {
              auto metall_ser = parquet_to_metall[parquet_ser];
              std::visit(
                  [&metall_edges, &metall_ser, rec](const auto& parquet_val) {
                    metall_edges->set(metall_ser, rec, parquet_val);
                  },
                  parquet_val);
            }  // for loop
          }  // if holds_alternative
        });  // for_all

  }  // for schema
  return to_return;
}

// void metall_graph::compute_in_degree(std::string_view out_name) {
//   if (!out_name.starts_with("node.")) {
//     m_comm.cerr0("Invalid series name: ", out_name);
//     return;
//   }
//   if (!has_node_series(out_name)) {
//     m_comm.cerr0("Series name does not exist: ", out_name);
//     return;
//   }

//   // stopped here 2025-10-07
//   m_pdirected_edges.for_all_
// }

}  // namespace metalldata