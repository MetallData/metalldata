// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// TODO: metall_graph variants should more closely mirror multiseries variants.
#pragma once

#include <any>
#include <functional>
#include <variant>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <metall/metall.hpp>
#include <multiseries/multiseries_record.hpp>
#include <ygm/comm.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <boost/json.hpp>
#include "ygm/container/set.hpp"

namespace bjsn = boost::json;

/* ASSUMPTIONS
Everything is a multigraph
2 multi_series (vertices, edges)

internally hardcode u,v has primary col names in edge tables

Edges are not partitioned by u/v hashing

Vertex ids are always string,  col name in vertex dataframe is 'id'.
Vertices are partitioned by has of id

*/

namespace metalldata {

inline bool is_node_selector(std::string_view sel) {
  return sel.starts_with("node.");
}

inline bool is_edge_selector(std::string_view sel) {
  return sel.starts_with("edge.");
}

class metall_graph {
 private:
  using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
  using string_store_type = record_store_type::string_store_type;

  using record_id_type = record_store_type::record_id_type;

 public:
  using data_types =
    std::variant<size_t, double, bool, std::string, std::monostate>;
  const std::string           U_COL                 = "edge.u";
  const std::string           V_COL                 = "edge.v";
  const std::string           DIR_COL               = "edge.directed";
  const std::string           NODE_COL              = "node.id";
  const std::set<std::string> RESERVED_COLUMN_NAMES = {DIR_COL, U_COL, V_COL};

  /**
   * @brief Return code struct for methods
   *
   */
  struct return_code {
    std::map<std::string, size_t>   warnings;
    std::map<std::string, std::any> return_info;
    std::string                     error;
    bool                            good() const { return error.empty(); }
    operator bool() const { return good(); }

    // merges warnings from another return code. If the keys match, the
    // numbers are incremented.
    void merge_warnings(return_code other) {
      for (const auto& [msg, count] : other.warnings) {
        warnings[msg] += count;
      }
    }
  };

  /// if the where_clause is default constructed, m_has_predicate is false,
  /// which means:
  // 1) is_node_clause and is_edge_clause are both true
  // 2) evaluate() will always return true
  // TODO: get rid of node and edge differentiation.
  // TODO: get rid of initialized
  struct where_clause {
    where_clause();

    where_clause(const std::vector<std::string>&                     s_names,
                 std::function<bool(const std::vector<data_types>&)> pred);
    where_clause(const bjsn::value& jlrule);

    where_clause(const std::string& jsonlogic_file_path);

    where_clause(std::istream& jsonlogic_stream);

    const std::vector<std::string>& series_names() const {
      return m_series_names;
    }

    bool good() const {
      if (m_series_names.empty()) {
        return true;
      }
      auto first_part = get_first_part(m_series_names.front());

      for (const auto& name : m_series_names) {
        if (first_part != get_first_part(name)) {
          return false;
        }
      }
      return true;
    }

    bool is_node_clause() const {
      return !m_series_names.empty() && good() &&
             get_first_part(m_series_names.front()) == "node";
    }

    bool is_edge_clause() const {
      return !m_series_names.empty() && good() &&
             get_first_part(m_series_names.front()) == "edge";
    }

    const auto& predicate() const { return m_predicate; }

    bool evaluate(const std::vector<data_types>& data) const {
      return m_predicate(data);
    }

    bool empty() const { return m_series_names.empty(); }

   private:
    std::vector<std::string>                            m_series_names;
    std::function<bool(const std::vector<data_types>&)> m_predicate;

    std::string get_first_part(const std::string& str) const {
      size_t pos = str.find('.');
      if (pos != std::string::npos) {
        return str.substr(0, pos);
      }
      return str;  // Return the whole string if no period is found
    }
  };

  /*

    where = node.age > 21 && node.zipcode = 77845

    where = edge.u.age > edge.v.age

    unsuupported =  (node.age > 21 && node.zipcode = 77845) & (edge.u.age >
    edge.v.age)


  */

  metall_graph(ygm::comm& comm, std::string_view path, bool overwrite = false);

  ~metall_graph();

  //
  // Ingest from parquet, provide 2 column names to define an edge, provide if
  // directed, provide list of optional metadata fields
  return_code ingest_parquet_edges(std::string_view path, bool recursive,
                                   std::string_view col_u,
                                   std::string_view col_v, bool directed,
                                   const std::vector<std::string>& meta);

  return_code ingest_parquet_verts(std::string_view path, bool recursive,
                                   std::string_view                key,
                                   const std::vector<std::string>& meta,
                                   bool overwrite = true);

  return_code ingest_ndjson_edges(std::string_view path, bool recursive,
                                  std::string_view col_u,
                                  std::string_view col_v, bool directed,
                                  const std::vector<std::string>& meta);

  return_code ingest_ndjson_verts(std::string_view path, bool recursive,
                                  std::string_view                key,
                                  const std::vector<std::string>& meta,
                                  bool overwrite = true);

  std::map<std::string, std::string> get_edge_selector_info() {
    // Since the m_pedges schema is identical across ranks, we don't have to
    // collect. Also: the "edge" prefix (and "node" in the corresponding
    // function) need to match the corresponding meta.json values.
    std::map<std::string, std::string> sels;
    for (const auto& el : m_pedges->get_series_names()) {
      auto sel = el;
      if (!el.starts_with("edge")) {
        sel = std::format("edge.{}", el);
      }
      sels[sel] = "default";
    }

    return sels;
  }

  std::map<std::string, std::string> get_node_selector_info() {
    // Since the m_pedges schema is identical across ranks, we don't have to
    // collect.
    std::map<std::string, std::string> sels;
    for (const auto& el : m_pnodes->get_series_names()) {
      auto sel = el;
      if (!el.starts_with("node")) {
        sel = std::format("node.{}", el);
      }
      sels[sel] = "default";
    }
    return sels;
  }

  std::map<std::string, std::string> get_selector_info() {
    std::map<std::string, std::string> sels = get_edge_selector_info();

    std::map<std::string, std::string> nsels = get_node_selector_info();
    sels.insert(nsels.begin(), nsels.end());

    return sels;
  }

  template <typename T>
  bool add_series(std::string_view name) {  // "node.color" or "edge.time"
    if (name.starts_with("node.")) {
      if (has_node_series(name)) {
        return false;
      }
      m_pnodes->add_series<T>(name);
      return true;
    }
    if (name.starts_with("edge.")) {
      if (has_edge_series(name)) {
        return false;
      }
      m_pedges->add_series<T>(name);
      return true;
    }
    return false;
  }

  bool drop_series(const std::string& name);

  bool has_node_series(std::string_view name) const {
    return m_pnodes->contains_series(name);
  };

  bool has_edge_series(std::string_view name) const {
    return m_pedges->contains_series(name);
  };

  bool has_series(std::string_view name) const {
    return has_node_series(name) || has_edge_series(name);
  };

  std::vector<std::string> get_node_series_names() const {
    return m_pnodes->get_series_names();
  };

  std::vector<std::string> get_edge_series_names() const {
    return m_pedges->get_series_names();
  };

  size_t num_edges(const where_clause& where = where_clause{}) const {
    size_t local_size = local_num_edges();
    if (!where.empty()) {
      local_size = 0;
      for_all_edges([&](auto) { ++local_size; }, where);
    }
    return ygm::sum(local_size, m_comm);
  }

  size_t num_nodes(const where_clause& where = where_clause{}) const {
    size_t local_size = local_num_nodes();
    if (!where.empty()) {
      local_size = 0;
      for_all_nodes([&](auto) { ++local_size; }, where);
    }
    return ygm::sum(local_size, m_comm);
  }

  size_t num_node_series() const { return m_pnodes->num_series(); };

  size_t num_edge_series() const { return m_pedges->num_series(); };

  template <typename Fn>
  void visit_node_field(std::string_view series_name, size_t record_id,
                        Fn func) const {
    m_pnodes->visit_field(series_name, record_id, func);
  }

  template <typename Fn>
  void visit_edge_field(std::string_view series_name, size_t record_id,
                        Fn func) const {
    m_pedges->visit_field(series_name, record_id, func);
  }

  /**
   * @brief Determines if the metall_graph is in good condition
   *
   * @return true metall_graph is valid
   * @return false metall_graph is invalid
   */
  bool good() const { return m_pmetall_mpi != nullptr; }

  operator bool() const { return good(); }

  // The following for_all functions take a function that
  // is passed the index as a parameter:
  // Fn: [](record_id_type record_id) {}
  // TODO: need to accept node where clauses. This is tricky. Leave for Roger.
  template <typename Fn>
  void for_all_edges(Fn                  func,
                     const where_clause& where = where_clause()) const {
    // take the where clause. Convert the where clause variables to
    // a vector of series indices. If it's missing, throw runtime.
    //

    auto var_idxs_o = m_pedges->find_series(where.series_names());
    if (!var_idxs_o.has_value()) {
      return;
    }

    auto var_idxs = var_idxs_o.value();
    auto wrapper  = [&](size_t row_index) {
      std::vector<data_types> var_data;
      var_data.reserve(var_idxs.size());
      for (auto series_idx : var_idxs) {
        auto val = m_pedges->get_dynamic(series_idx, row_index);
        std::visit(
          [&var_data](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int64_t>) {
              var_data.push_back(size_t(v));
            } else if constexpr (std::is_same_v<T, std::string_view>) {
              var_data.push_back(std::string(v));
            } else {
              var_data.push_back(v);
            }
          },
          val);
      }

      if (where.evaluate(var_data)) {
        func(row_index);
      }
    };
    m_pedges->for_all_rows(wrapper);
  };

  // for_all_nodes lambda takes a row index.
  template <typename Fn>
  void for_all_nodes(Fn func, const where_clause& where) const {
    if (where.is_node_clause()) {
      auto var_idxs_o = m_pnodes->find_series(where.series_names());
      if (!var_idxs_o.has_value()) {
        return;
      }
      auto var_idxs = var_idxs_o.value();

      auto wrapper = [&](size_t row_index) {
        std::vector<data_types> var_data;
        var_data.reserve(var_idxs.size());
        for (auto series_idx : var_idxs) {
          auto val = m_pnodes->get_dynamic(series_idx, row_index);
          std::visit(
            [&var_data](const auto& v) {
              using T = std::decay_t<decltype(v)>;
              if constexpr (std::is_same_v<T, int64_t>) {
                var_data.push_back(size_t(v));
              } else if constexpr (std::is_same_v<T, std::string_view>) {
                var_data.push_back(std::string(v));
              } else {
                var_data.push_back(v);
              }
            },
            val);
        }

        if (where.evaluate(var_data)) {
          func(row_index);
        }
      };

      m_pnodes->for_all_rows(wrapper);
    } else if (where.is_edge_clause()) {
      auto u_col_idx = m_pedges->find_series(U_COL);
      auto v_col_idx = m_pedges->find_series(V_COL);

      ygm::container::set<std::string> nodeset(m_comm);
      for_all_edges(
        [&](record_id_type record_idx) {
          auto u = m_pedges->get<std::string_view>(u_col_idx, record_idx);
          auto v = m_pedges->get<std::string_view>(v_col_idx, record_idx);

          nodeset.async_insert(std::string(u));
          nodeset.async_insert(std::string(v));
        },
        where);

      std::unordered_map<std::string, record_id_type> node_to_id;
      auto node_col_idx = m_pnodes->find_series(NODE_COL);
      m_pnodes->for_all_rows([&](record_id_type rid) {
        auto name = m_pnodes->get<std::string_view>(node_col_idx, rid);

        node_to_id[std::string(name)] = rid;
      });

      for (const auto& node : nodeset) {
        // throw an exception if the node is not in our node dataframe.
        func(node_to_id.at(node));
      }
    }
  }

  return_code in_degree(std::string_view out_name,
                        const where_clause& = where_clause());

  return_code out_degree(std::string_view out_name,
                         const where_clause& = where_clause());

  return_code degrees(std::string_view in_name, std::string_view out_name,
                      const where_clause& = where_clause());

  return_code degrees2(std::string_view in_name, std::string_view out_name,
                       const where_clause& = where_clause());

  return_code connected_components(std::string_view out_name,
                                   const where_clause& = where_clause());

  struct ego_net_options {
    std::optional<std::string> v_dist_closest;
    std::optional<std::string> v_closest_source;
    std::optional<std::string> e_included;
    std::optional<std::string> v_included;
  };

  return_code nhops(std::vector<std::string> sources, int hops, ego_net_options,
                    const where_clause& = where_clause());
  return_code nhops(std::vector<std::string> sources, int hops, bool half_hop,
                    ego_net_options, const where_clause& = where_clause());

  // TODO: also allow val a function
  return_code assign(std::string_view series_name, const data_types& val,
                     const where_clause& = where_clause());

  return_code erase(const where_clause& = where_clause());

  // struct shortest_path_options {
  //   std::optional<std::string> dist_series;
  //   std::optional<std::string> parent_series;
  //   std::optional<std::string> parent_count_series;
  // };
  // shortest_path_options opts;

  // return_code shortest_path(std::string source, shortest_path_options
  // opts,
  //                           const where_clause& = where_clause());
  // return_code shortest_path(std::vector<std::string> sources,
  //                           shortest_path_options    opts,
  //                           const where_clause& = where_clause());
  // return_code shortest_path(std::string source, std::string weights,
  //                           shortest_path_options ops,
  //                           const where_clause& = where_clause());

 private:
  std::string m_metall_path;  ///< Path to underlying metall storage
  ygm::comm&  m_comm;         ///< YGM Comm

  metall::utility::metall_mpi_adaptor* m_pmetall_mpi = nullptr;

  /// Dataframe for vertex metadata
  record_store_type* m_pnodes = nullptr;
  /// Dataframe for directed edges
  record_store_type* m_pedges = nullptr;

  size_t local_num_nodes() const { return m_pnodes->num_records(); };
  size_t local_num_edges() const { return m_pedges->num_records(); };

  return_code in_out_degree(std::string_view series_name, const where_clause&,
                            bool             outdeg);

  // Sets a node metadata column based on a lookup from an associative data
  // structure.
  // Node names are extracted from the key.

  // collection is some sort of associative data structure that has
  // key->value mappings. the key is the node name. Keys that do not correspond
  // to nodes are ignored, but if the number is greater than zero, a warning
  // message will be added to the return code.
  //
  // TODO: memoize / persist the node_to_id map so that we're not building it
  // every time.
  template <typename T>
  return_code set_node_column(std::string_view nodecol_name,
                              const T&         collection) {
    return_code to_return;

    using record_id_type = record_store_type::record_id_type;
    using val_type       = typename T::mapped_type;

    // create a node_local map of record id to node value.
    std::map<std::string, record_id_type> node_to_id{};
    m_pnodes->for_all_rows([&](record_id_type id) {
      std::string_view node = m_pnodes->get<std::string_view>(NODE_COL, id);
      node_to_id[std::string(node)] = id;
    });

    // create series and store index so we don't have to keep looking it up.
    auto nodecol_idx = m_pnodes->add_series<val_type>(nodecol_name);

    size_t invalid_nodes = 0;
    for (const auto& [k, v] : collection) {
      if (!node_to_id.contains(k)) {
        ++invalid_nodes;
        continue;
      }
      auto node_idx = node_to_id.at(k);
      m_pnodes->set(nodecol_idx, node_idx, v);
    }

    if (invalid_nodes > 0) {
      to_return.warnings["invalid nodes"] = invalid_nodes;
    }

    return to_return;
  }
};  // class metall_graph
}  // namespace metalldata