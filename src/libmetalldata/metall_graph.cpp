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

/**
 * @brief Converts a JSONLogic rule into a lambda for use within a where clause.
 *
 * @param jl_rule A boost::json::value containing the JSONLogic rule
 * @return A tuple containing the compiled lambda function and a vector of
 * variable names
 */

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
    add_series<std::string_view>(DIR_COL);

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

metall_graph::return_code metall_graph::ingest_parquet_edges(
  std::string_view path, bool recursive, std::string_view col_u,
  std::string_view col_v, bool directed,
  const std::optional<std::vector<series_name>>& meta) {
  return_code to_return;
  // Note: meta is exclusive of col_u and col_v. The metaset should
  // consist of qualified selector names (start with node. or edge.)
  // The parquet file, since it deals with edge data only, should use
  // unqualified selector names.
  // Setup parquet reader

  std::vector<std::string> paths;
  paths.push_back(path.data());
  ygm::io::parquet_parser parquetp(m_comm, paths, recursive);
  const auto&             schema = parquetp.get_schema();

  std::vector<std::string> parquet_cols;
  parquet_cols.reserve(schema.size());
  for (size_t i = 0; i < schema.size(); ++i) {
    auto& n = schema[i].name;
    parquet_cols.emplace_back(n);
  }

  std::set<series_name> metaset;
  if (meta.has_value()) {
    auto& v = meta.value();
    metaset = {v.begin(), v.end()};
  } else {
    for (const auto& col : parquet_cols) {
      if (col != col_u && col != col_v) {
        series_name sn = {"edge", col};
        metaset.insert(sn);
      }
    }
  }

  ///\todo eliminate nodeset, after completing persistent node to index map
  ygm::container::set<std::string> nodeset(m_comm);

  for (const auto& name : RESERVED_COLUMN_NAMES) {
    if (metaset.contains(name)) {
      to_return.error =
        "Error: reserved name " + name.qualified() + " found in meta data.";
      return to_return;
    }
  }

  metaset.emplace(series_name{"edge", col_u});
  metaset.emplace(series_name{"edge", col_v});

  std::map<std::string, series_name> parquet_to_metall;

  bool got_u = false;
  bool got_v = false;

  // we have parquet_cols already
  for (size_t i = 0; i < schema.size(); ++i) {
    std::string pcol_name = schema[i].name;
    auto        pcol_type = schema[i].type;
    series_name mapped_name{"edge", pcol_name};
    if (metaset.contains(mapped_name)) {
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
      bool        add_series_err = false;
      // Don't try to add series for U_COL and V_COL - they already exist
      if (pcol_name != col_u && pcol_name != col_v &&
          !has_series(mapped_name)) {
        if (pcol_type.equal(parquet::Type::BOOLEAN)) {
          add_series_err = !add_series<bool>(mapped_name);
        } else if (pcol_type.equal(parquet::Type::INT32) ||
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
    };
  }  // for schema

  if (!got_u) {
    to_return.error = "did not find u column: " + std::string(col_u);
    return to_return;
  }

  if (!got_v) {
    to_return.error = "did not find v column: " + std::string(col_v);
    return to_return;
  }

  if (!has_edge_series(DIR_COL)) {
    if (!add_series<bool>(DIR_COL)) {
      to_return.error = "could not add directed column";
      return to_return;
    }
  }

  auto metall_edges = m_pedges;

  size_t local_num_edges = 0;
  parquetp.for_all(
    parquet_cols,
    [&](const std::vector<ygm::io::parquet_parser::parquet_type_variant>& row) {
      auto rec = metall_edges->add_record();
      // first, set the directedness.
      metall_edges->set(DIR_COL.unqualified(), rec, directed);
      for (size_t i = 0; i < parquet_cols.size(); ++i) {
        auto parquet_ser = parquet_cols[i];

        // Skip columns that aren't in parquet_to_metall (not in metaset)
        if (!parquet_to_metall.contains(parquet_ser)) {
          continue;
        }

        auto parquet_val = row[i];

        auto metall_ser = parquet_to_metall[parquet_ser];

        auto add_val = [&](const auto& val) {
          using T = std::decay_t<decltype(val)>;

          // these are overrides for static_cast
          if constexpr (std::is_same_v<T, std::monostate>) {
            // do nothing
          } else if constexpr (std::is_same_v<T, int>) {
            metall_edges->set(metall_ser.unqualified(), rec,
                              static_cast<int64_t>(val));
          } else if constexpr (std::is_same_v<T, long>) {
            metall_edges->set(metall_ser.unqualified(), rec,
                              static_cast<int64_t>(val));
          } else if constexpr (std::is_same_v<T, float>) {
            metall_edges->set(metall_ser.unqualified(), rec,
                              static_cast<double>(val));
          } else if constexpr (std::is_same_v<T, std::string>) {
            metall_edges->set(metall_ser.unqualified(), rec,
                              std::string_view(val));
            // if this is u or v, add to the distributed nodeset.
            if (metall_ser == U_COL || metall_ser == V_COL) {
              nodeset.async_insert(val);
            }
          } else {
            metall_edges->set(metall_ser.unqualified(), rec, val);
          };
          ++local_num_edges;
        };
        std::visit(add_val, parquet_val);
      }  // for loop
    });  // for_all

  // go through the local possible nodes to add and if we don't
  // have them, then add to the graph's m_pnodes. This starts with
  // a barrier so we don't need an explicit one beforehand.

  size_t local_num_nodes = m_pnode_to_idx->size();
  for (const auto& v : nodeset) {
    priv_local_node_find_or_insert(v);
  }

  to_return.return_info["num_edges_ingested"] =
    ygm::sum(local_num_edges, m_comm);
  to_return.return_info["num_new_nodes_ingested"] =
    ygm::sum(m_pnode_to_idx->size() - local_num_edges, m_comm);
  return to_return;
}

metall_graph::return_code metall_graph::out_degree(
  series_name out_name, const metall_graph::where_clause& where) {
  return priv_in_out_degree(out_name, where, true);
}

metall_graph::return_code metall_graph::in_degree(
  series_name in_name, const metall_graph::where_clause& where) {
  return priv_in_out_degree(in_name, where, false);
}

/**
 * @brief Private helper function for computing in-degree or out-degree.
 *
 * This is an internal helper used by in_degree() and out_degree() to
 * calculate degree values for nodes matching a where clause.
 *
 * @param series_name Name of the series to store degree values
 * @param where Where clause to filter nodes
 * @param outdeg If true, compute out-degree; if false, compute in-degree
 * @return return_code indicating success or failure
 */
metall_graph::return_code metall_graph::priv_in_out_degree(
  series_name name, const metall_graph::where_clause& where, bool outdeg) {
  using record_id_type = record_store_type::record_id_type;

  metall_graph::return_code to_return;
  series_name               degcol, otherdegcol;
  if (outdeg) {
    degcol      = U_COL;
    otherdegcol = V_COL;
  } else {
    degcol      = V_COL;
    otherdegcol = U_COL;
  }

  if (!name.is_node_series()) {
    to_return.error = std::format("Invalid series name: {}", name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(name.unqualified())) {
    to_return.error = std::format("Series {} already exists", name.qualified());
    return to_return;
  }

  auto                                      edges_ = m_pedges;
  ygm::container::counting_set<std::string> degrees(m_comm);
  priv_for_all_edges(
    [&](record_id_type id) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      std::string_view edge_name =
        m_pedges->get<std::string_view>(degcol.unqualified(), id);
      degrees.async_insert(std::string(edge_name));

      // for undirected edges, add the reverse.
      bool is_directed = m_pedges->get<bool>(DIR_COL.unqualified(), id);
      if (!is_directed) {
        auto reverseedge_name =
          m_pedges->get<std::string_view>(otherdegcol.unqualified(), id);
        degrees.async_insert(std::string(reverseedge_name));
      }
    },
    where);

  // not strictly required because the subsequent loop over degrees begins
  // with a barrier. But that's spooky action at a distance, so we will be
  // explicit here.
  m_comm.barrier();

  set_node_column(name, degrees);

  return to_return;
}

metall_graph::return_code metall_graph::degrees(
  series_name in_name, series_name out_name,
  const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;

  metall_graph::return_code to_return;

  if (!in_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", in_name.qualified());
    return to_return;
  }

  if (!out_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", out_name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(in_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", in_name.qualified());
    return to_return;
  }
  if (m_pnodes->contains_series(out_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", out_name.qualified());
    return to_return;
  }

  auto                                      edges_ = m_pedges;
  ygm::container::counting_set<std::string> indegrees(m_comm);
  ygm::container::counting_set<std::string> outdegrees(m_comm);
  priv_for_all_edges(
    [&](record_id_type id) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto in_edge_name =
        std::string(m_pedges->get<std::string_view>(V_COL.unqualified(), id));
      auto out_edge_name =
        std::string(m_pedges->get<std::string_view>(U_COL.unqualified(), id));
      indegrees.async_insert(in_edge_name);
      outdegrees.async_insert(out_edge_name);

      bool is_directed = m_pedges->get<bool>(DIR_COL.unqualified(), id);
      if (!is_directed) {
        indegrees.async_insert(out_edge_name);
        outdegrees.async_insert(in_edge_name);
      }
    },
    where);

  // not strictly required because the subsequent loop over degrees begins
  // with a barrier. But that's spooky action at a distance, so we will be
  // explicit here.
  m_comm.barrier();

  // TODO: we want to abstract this to set_node_column because this is a
  // common operation. Make this a private function inside metall_graph.

  // create a node_local map of record id to node value.
  std::map<std::string, record_id_type> node_to_id{};
  m_pnodes->for_all_rows([&](record_id_type id) {
    std::string_view node =
      m_pnodes->get<std::string_view>(NODE_COL.unqualified(), id);
    node_to_id[std::string(node)] = id;
  });

  // create series and store index so we don't have to keep looking it up.
  auto in_deg_idx  = m_pnodes->add_series<size_t>(in_name.unqualified());
  auto out_deg_idx = m_pnodes->add_series<size_t>(out_name.unqualified());

  // add the values to the degrees series. We are taking advantage of the fact
  // that the node information is local from the degrees shared counting set
  // because it uses the same partitioning scheme as we used when we added the
  // nodes in ingest.
  for (const auto& [k, v] : indegrees) {
    auto rec_idx = node_to_id.at(k);
    m_pnodes->set(in_deg_idx, rec_idx, v);
  }

  for (const auto& [k, v] : outdegrees) {
    auto rec_idx = node_to_id.at(k);
    m_pnodes->set(out_deg_idx, rec_idx, v);
  }

  return to_return;
}

metall_graph::return_code metall_graph::degrees2(
  series_name in_name, series_name out_name,
  const metall_graph::where_clause& where) {
  using record_id_type = record_store_type::record_id_type;

  metall_graph::return_code to_return;

  if (!in_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", in_name.qualified());
    return to_return;
  }

  if (!out_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", out_name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(in_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", in_name.qualified());
    return to_return;
  }
  if (m_pnodes->contains_series(out_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", out_name.qualified());
    return to_return;
  }

  auto                                      edges_ = m_pedges;
  ygm::container::counting_set<std::string> indegrees(m_comm);
  ygm::container::counting_set<std::string> outdegrees(m_comm);

  auto u_col   = m_pedges->find_series(U_COL.unqualified());
  auto v_col   = m_pedges->find_series(V_COL.unqualified());
  auto dir_col = m_pedges->find_series(DIR_COL.unqualified());

  priv_for_all_edges(
    [&](record_id_type id) {
      // Note: clangd may report a false positive error on the next line
      // The code compiles and runs correctly
      auto in_edge_name =
        std::string(m_pedges->get<std::string_view>(v_col, id));
      auto out_edge_name =
        std::string(m_pedges->get<std::string_view>(u_col, id));
      indegrees.async_insert(in_edge_name);
      outdegrees.async_insert(out_edge_name);

      bool is_directed = m_pedges->get<bool>(dir_col, id);
      if (!is_directed) {
        indegrees.async_insert(out_edge_name);
        outdegrees.async_insert(in_edge_name);
      }
    },
    where);

  // not strictly required because the subsequent loop over degrees begins
  // with a barrier. But that's spooky action at a distance, so we will be
  // explicit here.
  m_comm.barrier();

  to_return       = set_node_column(in_name, indegrees);
  auto to_return2 = set_node_column(out_name, outdegrees);
  to_return.merge_warnings(to_return2);

  return to_return;
}

metall_graph::return_code metall_graph::nhops(series_name              out_name,
                                              size_t                   nhops,
                                              std::vector<std::string> sources,
                                              const where_clause&      where) {
  return_code to_return;

  if (!out_name.is_node_series()) {
    to_return.error =
      std::format("Invalid series name: {}", out_name.qualified());
    return to_return;
  }

  if (m_pnodes->contains_series(out_name.unqualified())) {
    to_return.error =
      std::format("Series {} already exists", out_name.qualified());
    return to_return;
  }

  auto u_col           = m_pedges->find_series(U_COL.unqualified());
  auto v_col           = m_pedges->find_series(V_COL.unqualified());
  auto is_directed_col = m_pedges->find_series(DIR_COL.unqualified());
  // TODO: convert to (rank, node row id) tuples.
  ygm::container::map<std::string, std::vector<std::string>> adj_list(m_comm);

  priv_for_all_edges(
    [&](record_id_type id) {
      std::string u(m_pedges->get<std::string_view>(u_col, id));
      std::string v(m_pedges->get<std::string_view>(v_col, id));
      auto        is_directed = m_pedges->get<bool>(is_directed_col, id);
      auto adj_inserter = [](const std::string&, std::vector<std::string>& adj,
                             const std::string& vert) { adj.push_back(vert); };
      adj_list.async_visit(u, adj_inserter, v);
      if (!is_directed) {
        adj_list.async_visit(v, adj_inserter, u);
      }
    },
    where);

  std::map<std::string, size_t>    local_nhop_map;
  ygm::container::set<std::string> visited(m_comm, sources), cur_level(m_comm),
    next_level(m_comm, sources);
  size_t cur_level_dist = 0;

  static auto* sp_visited    = &visited;
  static auto* sp_next_level = &next_level;

  while (next_level.size() > 0) {
    cur_level.swap(next_level);
    next_level.clear();
    for (const std::string& v : cur_level) {
      local_nhop_map[v] = cur_level_dist;
      if (adj_list.local_count(v) > 0) {
        for (const auto& neighbor : adj_list.local_at(v)) {
          visited.async_contains(neighbor,
                                 [](bool found, const std::string& node) {
                                   if (!found) {
                                     sp_visited->local_insert(node);
                                     sp_next_level->local_insert(node);
                                   }
                                 });
        }
      }
    }

    ++cur_level_dist;
  }

  to_return = set_node_column(out_name, local_nhop_map);

  return to_return;
}

}  // namespace metalldata