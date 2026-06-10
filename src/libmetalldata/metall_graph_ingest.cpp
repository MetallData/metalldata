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

  for (const auto& name : detail::RESERVED_COLUMN_NAMES) {
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

  size_t u_col_idx;
  size_t v_col_idx;
  // we have parquet_cols already - it's mapped 1:1 to schema.
  for (size_t i = 0; i < schema.size(); ++i) {
    std::string pcol_name = schema[i].name;
    auto        pcol_type = schema[i].type;
    series_name mapped_name{"edge", pcol_name};
    if (metaset.contains(mapped_name)) {
      if (pcol_name == col_u) {
        // we're no longer checking to see what the pcol_type is since
        // we will be coercing.
        // YGM_ASSERT_RELEASE(pcol_type.equal(parquet::Type::BYTE_ARRAY));

        mapped_name = detail::U_COL;
        got_u = true;
        u_col_idx = i;
      } else if (pcol_name == col_v) {
        // see above
        // YGM_ASSERT_RELEASE(pcol_type.equal(parquet::Type::BYTE_ARRAY));
        mapped_name = detail::V_COL;
        got_v = true;
        v_col_idx = i;
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

  if (!has_edge_series(detail::DIR_COL)) {
    if (!add_series<bool>(detail::DIR_COL)) {
      to_return.error = "could not add directed column";
      return to_return;
    }
  }

  size_t               local_nedges = 0;
  static metall_graph* sthis = nullptr;
  sthis = this;
  parquetp.for_all(
    parquet_cols,
    [&](const std::vector<ygm::io::parquet_parser::parquet_type_variant>& row) {
      // for each row, check to make sure u and v are set. If they're
      // monostate/null, skip the entire row.

      auto u_val = row[u_col_idx];
      auto v_val = row[v_col_idx];

      if (std::holds_alternative<std::monostate>(u_val)) {
        to_return.warnings["invalid u value skipped"]++;
        return;
      }
      if (std::holds_alternative<std::monostate>(v_val)) {
        to_return.warnings["invalid v value skipped"]++;
        return;
      }

      auto rec = m_pedges->add_record();
      // first, set the directedness.
      priv_local_set_edge_field(m_dir_col_idx, local_edge_idx_type{rec},
                                directed);
      for (size_t i = 0; i < parquet_cols.size(); ++i) {
        auto parquet_ser = parquet_cols[i];

        // Skip columns that aren't in parquet_to_metall (not in metaset)
        if (!parquet_to_metall.contains(parquet_ser)) {
          continue;
        }

        auto parquet_val = row[i];

        auto metall_ser = parquet_to_metall[parquet_ser];
        // memoization since we use this a few times.
        bool is_u_or_v =
          (metall_ser == detail::U_COL || metall_ser == detail::V_COL);
        // an edge is invalid if we have a type coercion problem
        bool                             invalid_edge = false;
        std::optional<series_index_type> metall_ser_idx_o =
          m_pedges->find_series(metall_ser.unqualified());
        if (!metall_ser_idx_o.has_value()) {
          continue;
        }
        series_index_type metall_ser_idx = metall_ser_idx_o.value();

        auto add_val = [&](const auto& val) {
          using T = std::decay_t<decltype(val)>;

          // if we're dealing with the u column or the v column,
          // coerce the value to a string or log a warning, and
          // then add to the nodeset.
          if (is_u_or_v) {
            std::string uv_invalid =
              std::format("invalid {} value skipped",
                          metall_ser == detail::U_COL ? "u" : "v");

            // if monostate, just skip and log.
            if constexpr (std::is_same_v<T, std::monostate>) {
              to_return.warnings[uv_invalid]++;
              invalid_edge = true;
            } else {
              try {
                // first, stringify.
                std::string stringified_val =
                  std::format("{}", static_cast<T>(val));

                // next, set the stringified value
                m_pedges->set(metall_ser_idx, rec,
                              std::string_view(stringified_val));

                // next, add to the distributed nodeset.
                int owner = m_partitioner.owner(stringified_val);
                m_comm.async(
                  owner,
                  [](const std::string& s) {
                    sthis->priv_local_node_find_or_insert(s);
                  },
                  stringified_val);
                // finally, increase local_n_edges
                ++local_nedges;
              } catch (const std::exception) {
                // something went wrong with the try block. Skip.
                to_return.warnings[uv_invalid]++;
                invalid_edge = true;
              }
            }
          } else {  // not u column or v column; these can be any type.
            // these are overrides for static_cast
            if constexpr (std::is_same_v<T, std::monostate>) {
              // do nothing
            } else if constexpr (std::is_same_v<T, int>) {
              m_pedges->set(metall_ser_idx, rec, static_cast<int64_t>(val));
            } else if constexpr (std::is_same_v<T, long>) {
              m_pedges->set(metall_ser_idx, rec, static_cast<int64_t>(val));

            } else if constexpr (std::is_same_v<T, float>) {
              m_pedges->set(metall_ser_idx, rec, static_cast<double>(val));
            } else if constexpr (std::is_same_v<T, std::string>) {
              m_pedges->set(metall_ser_idx, rec, std::string_view(val));
            } else {
              m_pedges->set(metall_ser_idx, rec, val);
            };
          };
          if (!invalid_edge) ++local_nedges;
        };
        std::visit(add_val, parquet_val);
      }  // for loop
    });  // for_all

  m_comm.barrier();
  to_return.return_info["num_edges_ingested"] = ygm::sum(local_nedges, m_comm);
  to_return.return_info["num_new_nodes_ingested"] =
    ygm::sum(m_pnode_to_idx->size() - local_nedges, m_comm);
  return to_return;
}

}  // namespace metalldata