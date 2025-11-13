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
#include <cstdint>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>

#include "metall_graph.hpp"
#include "metall_jl/metall_jl.hpp"
#include <fcntl.h>

#include "boost/graph/graph_traits.hpp"
#include "multiseries/multiseries_record.hpp"
#include "ygm/container/set.hpp"

namespace metalldata {

/**
 * @brief Converts a JSONLogic rule into a lambda for use within a where clause.
 *
 * @param jl_rule A boost::json::value containing the JSONLogic rule
 * @return A tuple containing the compiled lambda function and a vector of
 * variable names
 */
static auto compile_jl_rule(bjsn::value jl_rule) {
  auto [expression_rule, vars_b, _] = jsonlogic::create_logic(jl_rule);

  std::vector<std::string> vars{vars_b.begin(), vars_b.end()};

  // Store the unique_ptr in a shared_ptr to make it copyable and shareable
  auto shared_expr =
    std::make_shared<jsonlogic::any_expr>(std::move(expression_rule));

  auto compiled =
    [shared_expr](std::vector<metall_graph::data_types> row) -> bool {
    // Convert data_types to value_variant
    std::vector<jsonlogic::value_variant> jl_row;
    jl_row.reserve(row.size());

    for (const auto& val : row) {
      std::visit(
        [&jl_row](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            jl_row.push_back(std::monostate{});
          } else if constexpr (std::is_same_v<T, bool>) {
            jl_row.push_back(arg);
          } else if constexpr (std::is_same_v<T, size_t>) {
            jl_row.push_back(static_cast<std::uint64_t>(arg));
          } else if constexpr (std::is_same_v<T, double>) {
            jl_row.push_back(arg);
          } else if constexpr (std::is_same_v<T, std::string>) {
            jl_row.push_back(std::string_view{arg});
          }
        },
        val);
    }

    auto res_j = jsonlogic::apply(*shared_expr, jl_row);
    return jsonlogic::unpack_value<bool>(res_j);
  };

  return std::make_tuple(compiled, vars);
}

metall_graph::where_clause::where_clause(bjsn::value jlrule) {
  auto [compiled, vars] = compile_jl_rule(jlrule);

  // Determine if this is a node or edge clause by checking the variable names
  bool has_node = false;
  bool has_edge = false;

  for (const auto& var : vars) {
    std::string var_str(var.c_str());
    if (is_node_selector(var_str)) {
      has_node = true;
    } else if (is_edge_selector(var_str)) {
      has_edge = true;
    }
    if (!(has_node ^ has_edge)) {
      throw std::runtime_error(
        "Expression must have exactly one type of selector (node or edge)");
    }
  }

  is_node = has_node;

  predicate    = compiled;
  series_names = vars;
}

metall_graph::metall_graph(ygm::comm& comm, std::string_view path,
                           bool overwrite)
    : m_comm(comm), m_metall_path(path) {
  //
  // Check if metall store already exists and overwrite if requested
  bool path_exists = std::filesystem::exists(path);
  if (!path_exists || overwrite) {
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
    m_pedges = manager.construct<record_store_type>("edges")(
      string_store, manager.get_allocator());

    // add the default series for the indices.
    add_series<std::string_view>(NODE_COL);
    add_series<std::string_view>(U_COL);
    add_series<std::string_view>(V_COL);

  } else {  // open existing
    comm.barrier();
    m_pmetall_mpi = new metall::utility::metall_mpi_adaptor(
      metall::open_read_only, m_metall_path, m_comm.get_mpi_comm());
    auto& manager = m_pmetall_mpi->get_local_manager();

    m_pnodes            = manager.find<record_store_type>("nodes").first;
    m_pedges            = manager.find<record_store_type>("edges").first;

    if (!m_pnodes || !m_pedges) {
      m_comm.cerr0(
        "Error: Failed to find required data structures in metall store");
      delete m_pmetall_mpi;
      m_pmetall_mpi       = nullptr;
      m_pnodes            = nullptr;
      m_pedges            = nullptr;
    }
  }

  YGM_ASSERT_RELEASE(has_node_series(NODE_COL));
  YGM_ASSERT_RELEASE(has_edge_series(U_COL));
  YGM_ASSERT_RELEASE(has_edge_series(V_COL));

  //
  // Open metall store
}

metall_graph::~metall_graph() {
  // Ensure all processors are together in the destructor
  m_comm.barrier();

  // We don't free these because they are persistent in the metall store
  m_pnodes            = nullptr;
  m_pedges            = nullptr;

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
    m_pedges->add_series<T>(name);
    return true;
  }
  return false;
}

bool metall_graph::drop_series(const std::string& name) {
  // TODO: does this need to check the prefix?
  if (RESERVED_COLUMN_NAMES.contains(name)) {
    m_comm.cerr0("Cannot remove reserved column ", name);
    return false;
  }
  if (name.starts_with("node.")) {
    return m_pnodes->remove_series(name);
  }
  return m_pedges->remove_series(name);
}

metall_graph::return_code metall_graph::ingest_parquet_edges(
  std::string_view path, bool recursive, std::string_view col_u,
  std::string_view col_v, bool directed, const std::vector<std::string>& meta) {
  return_code to_return;
  // Note: meta is exclusive of col_u and col_v.
  //
  // Setup parquet reader

  std::vector<std::string> paths;
  paths.push_back(path.data());
  ygm::io::parquet_parser parquetp(m_comm, paths, recursive);
  const auto&             schema = parquetp.get_schema();

  std::set<std::string>              metaset(meta.begin(), meta.end());

  ygm::container::set<std::string> nodeset(m_comm);
  std::unordered_set<std::string>  localnodes{};

  for (const auto& name : RESERVED_COLUMN_NAMES) {
    if (metaset.contains(name)) {
      to_return.error = "Error: reserved name " + name + " found in meta data.";
      return to_return;
    }
  }

  metaset.emplace(col_u);
  metaset.emplace(col_v);
  std::map<std::string, std::string> parquet_to_metall;

  std::vector<std::string> parquet_cols;
  parquet_cols.reserve(schema.size());

  bool got_u = false;
  bool got_v = false;

  for (size_t i = 0; i < schema.size(); ++i) {
    parquet_cols.emplace_back(schema[i].name);

    std::string pcol_name = schema[i].name;
    auto        pcol_type = schema[i].type;
    if (metaset.contains(pcol_name)) {
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
        add_series_err = !add_series<std::string_view>(mapped_name);
      } else {
        std::stringstream ss;
        ss << "Unsupported column type: " << schema[i].type;
        to_return.warnings[ss.str()]++;
      }

      if (add_series_err) {
        to_return.error = "Failed to add source column: " + pcol_name;
      }
    }
  }  // for schema

  if (!got_u) {
    to_return.error = "did not find u column: " + std::string(col_u);
    return to_return;
  }

  if (!got_v) {
    to_return.error = "did not find v column: " + std::string(col_v);
    return to_return;
  }

  if (!add_series<bool>(DIR_COL)) {
    to_return.error = "could not add directed column";
    return to_return;
  }

  auto metall_edges = m_pedges;

  auto _U_COL   = U_COL;
  auto _V_COL   = V_COL;
  auto _DIR_COL = DIR_COL;
  // for each row, set the metall data.

  parquetp.for_all(
    parquet_cols,
    [&parquet_cols, &parquet_to_metall, &metall_edges, directed, _DIR_COL,
     _U_COL, _V_COL, &nodeset](
      const std::vector<ygm::io::parquet_parser::parquet_type_variant>& row) {
      auto rec = metall_edges->add_record();
      // first, set the directedness.
      metall_edges->set(_DIR_COL, rec, directed);
      for (size_t i = 0; i < parquet_cols.size(); ++i) {
        auto parquet_ser = parquet_cols[i];
        auto parquet_val = row[i];

        auto metall_ser = parquet_to_metall[parquet_ser];

        auto add_val    = [&](const auto& val) {
          using T = std::decay_t<decltype(val)>;

          // these are overrides for static_cast
          if constexpr (std::is_same_v<T, std::monostate>) {
            // do nothing
          } else if constexpr (std::is_same_v<T, int>) {
            metall_edges->set(metall_ser, rec, static_cast<int64_t>(val));
          } else if constexpr (std::is_same_v<T, long>) {
            metall_edges->set(metall_ser, rec, static_cast<int64_t>(val));
          } else if constexpr (std::is_same_v<T, float>) {
            metall_edges->set(metall_ser, rec, static_cast<double>(val));
          } else if constexpr (std::is_same_v<T, std::string>) {
            metall_edges->set(metall_ser, rec, std::string_view(val));
            // if this is u or v, add to the distributed nodeset.
            if (metall_ser == _U_COL || metall_ser == _V_COL) {
              nodeset.async_insert(val);
            }
          } else {
            metall_edges->set(metall_ser, rec, val);
          };
        };
        std::visit(add_val, parquet_val);
      }  // for loop
    });  // for_all

  // do a barrier here to make sure the nodeset is synched.
  // create a local std::set containing the m_pnodes vertices.

  // m_pnodes->for_all<std::string>(NODE_COL, [&](int _, const auto& el) {
  // using T = std::decay_t<decltype(el)>;
  // if constexpr (std::is_same_v<T, std::string_view>) {
  //   localnodes.emplace(el);
  // }
  // });

  // go through the local possible nodes to add and if we don't
  // have them, then add to the graph's m_pnodes. This starts with
  // a barrier so we don't need an explicit one beforehand.
  for (const auto& v : nodeset) {
    if (!localnodes.contains(v)) {
      auto rec = m_pnodes->add_record();
      m_pnodes->set(NODE_COL, rec, std::string_view(v));
      localnodes.emplace(v);
    }
  };
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