// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

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

/* ASSUMPTIONS
Everything is a multigraph
3 multi_series (vertices, directed edges, undirected edges)

internally hardcode u,v has primary col names in edge tables

Edges are not partitioned by u/v hashing

Vertex ids are always string,  col name in vertex dataframe is 'id'.
Vertices are partitioned by has of id

*/

namespace metalldata {
class metall_graph {
 public:
  using data_types =
    std::variant<size_t, double, bool, std::string, std::monostate>;
  const char* U_COL = "edge.__u";
  const char* V_COL = "edge.__v";

  enum class EdgeType : uint8_t { DIRECTED = 1, UNDIRECTED = 2 };

  friend constexpr EdgeType operator|(EdgeType lhs, EdgeType rhs) {
    return static_cast<EdgeType>(static_cast<uint8_t>(lhs) |
                                 static_cast<uint8_t>(rhs));
  }

  friend constexpr EdgeType operator&(EdgeType lhs, EdgeType rhs) {
    return static_cast<EdgeType>(static_cast<uint8_t>(lhs) &
                                 static_cast<uint8_t>(rhs));
  }

  friend constexpr bool includes(EdgeType value, EdgeType flag) {
    return (value & flag) == flag;
  }

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
  };

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

  template <typename T>
  bool add_series(std::string_view name);  // "node.color" or "edge.time"

  bool drop_series(std::string_view name);

  bool has_node_series(std::string_view name) const {
    return m_pnodes->contains_series(name);
  };

  bool has_edge_series(std::string_view name) const {
    return m_pdirected_edges->contains_series(name);
  };

  bool has_series(std::string_view name) const {
    return has_node_series(name) || has_edge_series(name);
  };

  std::vector<std::string> get_node_series_names() const {
    return m_pnodes->get_series_names();
  };

  std::vector<std::string> get_directed_edge_series_names() const {
    return m_pdirected_edges->get_series_names();
  };

  std::vector<std::string> get_undirected_edge_series_names() const {
    return m_pundirected_edges->get_series_names();
  };

  std::vector<std::string> get_edge_series_names() const {
    return m_pdirected_edges->get_series_names();
  };

  size_t size(EdgeType edges_to_include = EdgeType::DIRECTED |
                                          EdgeType::UNDIRECTED) const {
    size_t local_size = 0;

    if (includes(edges_to_include, EdgeType::DIRECTED)) {
      local_size += local_num_directed_edges();
    }
    if (includes(edges_to_include, EdgeType::UNDIRECTED)) {
      local_size += local_num_undirected_edges();
    }

    return ygm::sum(local_size, m_comm);
  }

  size_t order() const {
    size_t local_order = local_num_nodes();
    return ygm::sum(local_order, m_comm);
  }

  size_t num_node_series() const { return m_pnodes->num_series(); };

  size_t num_directed_edge_series() const {
    return m_pdirected_edges->num_series();
  };

  size_t num_undirected_edge_series() const {
    return m_pundirected_edges->num_series();
  };

  /**
   * @brief Determines if the metall_graph is in good condition
   *
   * @return true metall_graph is valid
   * @return false metall_graph is invalid
   */
  bool good() const { return m_pmetall_mpi != nullptr; }

  operator bool() const { return good(); }

  void compute_in_degree(std::string_view out_name);

  struct where_clause {
    where_clause() = default;  // everything = [](std::vector <
                               // std::variants<TYPE>) -> bool { return ...; };

    where_clause(std::vector<std::string>,
                 std::function<bool(std::vector<data_types>)> pred) {}

    // where_clause(jsonlogic::any_expr jlrule) { /*decode*/ }

    bool is_node_clause() const { return is_node; }
    bool is_edge_clause() const { return !is_node_clause(); }

   private:
    std::vector<std::string>                     series_names;
    std::function<bool(std::vector<data_types>)> predicate;
    bool                                         is_node;
  };

  /*

    where = node.age > 21 && node.zipcode = 77845

    where = edge.u.age > edge.v.age

    unsuupported =  (node.age > 21 && node.zipcode = 77845) & (edge.u.age >
    edge.v.age)


  */

  return_code in_degree(std::string_view out_name,
                        const where_clause& = where_clause());

  return_code out_degree(std::string_view out_name,
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

  // also allow val a function
  template <typename T>
  return_code assign(std::string series_name, T val,
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

  using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
  using string_store_type = record_store_type::string_store_type;

  /// Dataframe for vertex metadata
  record_store_type* m_pnodes = nullptr;
  /// Dataframe for directed edges
  record_store_type* m_pdirected_edges = nullptr;
  /// Dataframe for undirected edges
  record_store_type* m_pundirected_edges = nullptr;

  size_t local_num_nodes() const { return m_pnodes->num_records(); };

  size_t local_num_directed_edges() const {
    return m_pdirected_edges->num_records();
  };

  size_t local_num_undirected_edges() const {
    return m_pundirected_edges->num_records();
  };
};
}  // namespace metalldata