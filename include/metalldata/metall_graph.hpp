// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// TODO: metall_graph variants should more closely mirror multiseries variants.
#pragma once

#include <any>
#include <cstddef>
#include <functional>
#include <utility>
#include <variant>
#include <map>
#include <string>
#include <string_view>
#include <vector>
#include <metall/metall.hpp>
#include <multiseries/multiseries_record.hpp>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <boost/json.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <metall/container/unordered_map.hpp>
#include <expected>
#include <optional>
#include <ygm/utility/assert.hpp>
#include <ygm/container/set.hpp>
#include <ygm/container/counting_set.hpp>
#include <metalldata/result.hpp>
#include <string_table/string_accessor.hpp>
#include <string_table/string_store.hpp>

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

class metall_graph {
 private:
  /// multiseries record store are the dataframes
  using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
  using record_id_type = record_store_type::record_id_type;
  using series_index_type = record_store_type::series_index_type;

  /// string table deduplicates strings
  using string_store_type = record_store_type::string_store_type;
  using string_table_accessor = compact_string::string_accessor;

  enum class local_node_idx_type : std::size_t;
  enum class local_edge_idx_type : std::size_t;
  enum class node_series_idx_type : std::size_t;
  enum class edge_series_idx_type : std::size_t;

 public:
  using series_types = multiseries::basic_record_store<>::series_type;
  using count_types =
    std::variant<std::monostate, bool, int64_t, double, std::string>;

  /// Forward declared, see impl/metall_graph_locator.ipp
  struct node_locator;
  /// Forward declared, see impl/metall_graph_locator.ipp
  struct edge_locator;

  /// Forward declared, see impl/metall_graph_series_name.hpp
  struct series_name;

  /// Forward declared, see impl/metall_graph_where.hpp
  struct where_clause;

  metall_graph(ygm::comm& comm, std::string_view path, bool overwrite = false);

  ~metall_graph();

  //
  // Ingest from parquet, provide 2 column names to define an edge, provide if
  // directed, provide list of optional metadata fields
  result<std::map<std::string, size_t>> ingest_parquet_edges(
    std::string_view path, bool recursive, std::string_view col_u,
    std::string_view col_v, bool directed,
    const std::optional<std::vector<series_name>>& meta = std::nullopt);

  result<std::map<std::string, std::any>> dump_parquet_verts(
    std::string_view path, const std::vector<series_name>& meta,
    bool overwrite = false);

  result<std::map<std::string, std::any>> dump_parquet_edges(
    std::string_view path, const std::vector<series_name>& meta,
    bool overwrite = false);

  result<> erase_edges(const where_clause& where);

  result<> erase_edges(const series_name&                     name,
                       boost::unordered_flat_set<std::string> haystack);

  template <typename Fn, typename T>  // defined in metall_graph_faker.hpp
  result<> add_faker_series(const metall_graph::series_name& name,
                            Fn faker_func, const where_clause& where);

  template <typename Compare = std::greater<void>>
  result<std::vector<std::vector<count_types>>> topk(
    size_t k, const series_name& ser_name,
    const std::vector<series_name>& ser_inc, Compare comp,
    const where_clause& where);

  std::map<std::string, std::string> get_edge_selector_info();

  std::map<std::string, std::string> get_node_selector_info();

  std::map<std::string, std::string> get_selector_info();

  template <typename T>
  bool add_series(const series_name& name);

  // drop_series requires a qualified selector name (starts with node. or
  // edge.)
  bool drop_series(const series_name& name);

  result<> rename_series(const series_name& old_name,
                         const series_name& new_name);

  // has_node_series requires an UNqualified (stripped) selector name.
  bool has_node_series(std::string_view unqualified_name) const;

  bool has_node_series(const series_name& name) const;

  bool has_edge_series(std::string_view unqualified_name) const;

  bool has_edge_series(const series_name& name) const;

  bool has_series(const series_name& name) const;

  std::vector<series_name> get_node_series_names() const;

  std::vector<series_name> get_edge_series_names() const;

  size_t num_edges(const where_clause& where) const;

  size_t num_nodes(const where_clause& where) const;

  size_t num_node_series() const { return m_pnodes->num_series(); };

  size_t num_edge_series() const { return m_pedges->num_series(); };

  std::map<metall_graph::series_name, size_t> nunique_edge(
    std::unordered_set<metall_graph::series_name> series_names,
    const where_clause&                           where);

  std::map<metall_graph::series_name, size_t> nunique_node(
    std::unordered_set<metall_graph::series_name> series_names,
    const where_clause&                           where);

  ygm::container::counting_set<metall_graph::count_types> value_counts(
    metall_graph::series_name sname, const where_clause& where);

  std::map<metall_graph::count_types, size_t> value_counts_topk(
    metall_graph::series_name sname, int k, const where_clause& where);

  // TODO:  Remove this, used by select...  See:  impl/metall_graph_series.ipp
  template <typename Fn>
  void visit_node_field(const series_name& name, size_t record_id,
                        Fn func) const;

  // TODO:  Remove this, used by select...  See:  impl/metall_graph_series.ipp
  template <typename Fn>
  void visit_edge_field(const series_name& name, size_t record_id,
                        Fn func) const;

  result<boost::json::array> select_edges(
    const std::unordered_set<metall_graph::series_name>& series_set,
    const metall_graph::where_clause& where, size_t limit);

  result<boost::json::array> select_nodes(
    const std::unordered_set<metall_graph::series_name>& series_set,
    const metall_graph::where_clause& where, size_t limit);

  /**
   * @brief Determines if the metall_graph is in good condition
   *
   * @return true metall_graph is valid
   * @return false metall_graph is invalid
   */
  bool good() const { return m_pmetall_mpi != nullptr; }

  operator bool() const { return good(); }

  result<> in_degree(series_name out_name, const where_clause& where);

  result<> out_degree(series_name out_name, const where_clause& where);

  result<> degrees(series_name in_name, series_name out_name,
                   const where_clause& where);

  result<> degrees2(series_name in_name, series_name out_name,
                    const where_clause& where);

  result<> nhops(const series_name& out_node_series, size_t nhops,
                 const std::vector<std::string>& sources,
                 const where_clause&             where);

  result<> connected_components(const series_name&  out_node_series,
                                const where_clause& where);

  // TODO: also allow val a function
  result<> assign(series_name series_name, const series_types& val,
                  const where_clause& where);

  result<> sample_edges(const series_name& series_name, size_t k,
                        std::optional<uint64_t> optseed,
                        const where_clause&     where);

  bjsn::array select_sample_edges(
    size_t k, const std::vector<metall_graph::series_name>& metadata,
    std::optional<uint64_t> optseed, const metall_graph::where_clause& where);

  result<> sample_nodes(const series_name& series_name, size_t k,
                        std::optional<uint64_t> optseed,
                        const where_clause&     where);

  bjsn::array select_sample_nodes(
    size_t k, const std::vector<metall_graph::series_name>& metadata,
    std::optional<uint64_t> optseed, const metall_graph::where_clause& where);

 private:
  // TODO:  debug why we can't used string_accessor_fast_hash for these maps.
  /// hash table from node string label to local id.  For local nodes only.
  using map_local_node_to_local_id_type = boost::unordered::unordered_flat_map<
    string_table_accessor, local_node_idx_type,
    compact_string::string_accessor_hasher,
    std::equal_to<compact_string::string_accessor>,
    metall::manager::allocator_type<
      std::pair<const compact_string::string_accessor, local_node_idx_type>>>;

  /// hash table from node string label to global locator.  For tracking remote
  /// nodes.
  using map_node_to_locator_type = boost::unordered::unordered_flat_map<
    string_table_accessor, node_locator, compact_string::string_accessor_hasher,
    std::equal_to<compact_string::string_accessor>,
    metall::manager::allocator_type<
      std::pair<const compact_string::string_accessor, node_locator>>>;

  std::string m_metall_path;  ///< Path to underlying metall storage
  ygm::comm&  m_comm;         ///< YGM Comm

  metall::utility::metall_mpi_adaptor* m_pmetall_mpi = nullptr;

  /// Dataframe for vertex metadata
  record_store_type* m_pnodes = nullptr;
  /// Dataframe for directed edges
  record_store_type* m_pedges = nullptr;
  /// Map from vertex string to local nide id
  map_local_node_to_local_id_type* m_pnode_to_idx = nullptr;
  /// Map from vertex string to node locator
  map_node_to_locator_type* m_pnode_to_locator = nullptr;
  /// String store
  string_store_type* m_pstring_store = nullptr;

  edge_series_idx_type m_u_col_idx;
  edge_series_idx_type m_v_col_idx;
  edge_series_idx_type m_dir_col_idx;
  node_series_idx_type m_node_col_idx;

  /**
   * @brief Returns an edge's endpoints (u,v) as string_views
   *
   * @param eid Edge ID
   * @return std::optional<std::pair<std::string_view, std::string_view>>
   */
  std::optional<std::pair<std::string_view, std::string_view>>
  priv_local_get_edge_uv_labels(local_edge_idx_type eid) const;

  /**
   * @brief Returns an edge's directed field
   *
   * @param eid Edge Id
   * @return std::optional<bool>
   */
  std::optional<bool> priv_local_edge_is_directed(
    local_edge_idx_type eid) const;

  /**
   * @brief Retuns a node's string label
   *
   * @param nid Node id
   * @return std::optional<std::string_view>
   */
  std::optional<std::string_view> priv_local_get_node_label(
    local_node_idx_type nid) const;

  /**
   * @brief Returns an individual node field as a series_type variant
   *
   * @param sid Node series id
   * @param nid Node id
   * @return std::optional<series_types>
   */
  std::optional<series_types> priv_local_get_node_field(
    node_series_idx_type sid, local_node_idx_type nid) const;

  /**
   * @brief Returns an individual node field as a concrete type
   *
   * @tparam T
   * @param sid Node series id
   * @param nid Node id
   * @return std::optional<T>
   */
  template <typename T>
  std::optional<T> priv_local_get_node_field(node_series_idx_type sid,
                                             local_node_idx_type  nid) const;

  std::vector<std::optional<series_types>> priv_local_get_node_fields(
    std::vector<node_series_idx_type> sids, local_node_idx_type eid) const {
    std::vector<std::optional<series_types>> fields;
    fields.reserve(sids.size());
    for (const auto& s : sids) {
      fields.emplace_back(priv_local_get_node_field(s, eid));
    }
    return fields;
  }

  std::optional<series_types> priv_local_get_edge_field(
    edge_series_idx_type sid, local_edge_idx_type eid) const {
    return m_pedges->get_dynamic(std::to_underlying(sid),
                                 std::to_underlying(eid));
  }
  template <typename T>
  std::optional<T> priv_local_get_edge_field(edge_series_idx_type sid,
                                             local_edge_idx_type  eid) const {
    auto f = priv_local_get_edge_field(sid, eid);
    if (f.has_value()) {
      if (std::holds_alternative<T>(f.value())) {
        return std::get<T>(f.value());
      }
    }
    return std::nullopt;
  }

  std::vector<std::optional<series_types>> priv_local_get_edge_fields(
    std::vector<edge_series_idx_type> sids, local_edge_idx_type eid) const {
    std::vector<std::optional<series_types>> fields;
    fields.reserve(sids.size());
    for (const auto& s : sids) {
      fields.emplace_back(priv_local_get_edge_field(s, eid));
    }
    return fields;
  }

  template <typename T>
  void priv_local_set_node_field(node_series_idx_type sid,
                                 local_node_idx_type nid, const T& val) {
    m_pnodes->set(std::to_underlying(sid), std::to_underlying(nid), val);
  }

  template <typename T>
  void priv_local_set_edge_field(edge_series_idx_type sid,
                                 local_edge_idx_type eid, const T& val) {
    m_pedges->set(std::to_underlying(sid), std::to_underlying(eid), val);
  }

  std::optional<node_series_idx_type> priv_local_find_node_series(
    std::string_view name) const {
    auto ret = m_pnodes->find_series(name);
    if (ret.has_value()) {
      return node_series_idx_type{
        static_cast<node_series_idx_type>(ret.value())};
    }
    return std::nullopt;
  }

  std::vector<std::optional<node_series_idx_type>> priv_local_find_node_series(
    std::vector<series_name> names) const;

  // TODO: this should probably take a series_name as an argument.
  std::optional<edge_series_idx_type> priv_local_find_edge_series(
    std::string_view name) const;

  std::vector<std::optional<edge_series_idx_type>> priv_local_find_edge_series(
    const std::vector<series_name>& names) const;

  template <typename T>
  node_series_idx_type priv_add_node_series(std::string_view name) {
    return node_series_idx_type{m_pnodes->add_series<T>(name)};
  }

  template <typename T>
  edge_series_idx_type priv_add_edge_series(std::string_view name) {
    return edge_series_idx_type{m_pedges->add_series<T>(name)};
  }

  template <typename T>
  bool priv_is_node_series_type(node_series_idx_type ns) {
    return m_pnodes->is_series_type<T>(std::to_underlying(ns));
  }

  template <typename T>
  bool priv_is_edge_series_type(edge_series_idx_type es) {
    return m_pedges->is_series_type<T>(std::to_underlying(es));
  }

  size_t priv_local_num_nodes() const { return m_pnodes->num_records(); };
  size_t priv_local_num_edges() const { return m_pedges->num_records(); };

  result<> priv_in_out_degree(series_name name, const where_clause& where,
                              bool outdeg);

  template <typename Fn>
  void priv_for_all_edges(Fn func) const;

  template <typename Fn>
  void priv_for_all_edges(Fn func, const where_clause& where) const;

  template <typename Fn>
  void priv_for_all_edges_nwhere(Fn func, const where_clause& where) const;

  template <typename Fn>
  void priv_for_all_edges_ewhere(Fn func, const where_clause& where) const;

  template <typename Fn>
  void priv_for_all_nodes(Fn func) const;

  template <typename Fn>
  void priv_for_all_nodes(Fn func, const where_clause& where) const;

  template <typename Fn>
  void priv_for_all_nodes_nwhere(Fn func, const where_clause& where) const;

  template <typename Fn>
  void priv_for_all_nodes_ewhere(Fn func, const where_clause& where) const;

  std::pair<std::vector<local_node_idx_type>, std::vector<local_edge_idx_type>>
  priv_where_subgraph(const where_clause& where) const;

  // Sets a node metadata column based on a lookup from an associative data
  // structure.
  // Node names are extracted from the key.

  // collection is some sort of associative data structure that has
  // key->value mappings. the key is the node name. Keys that do not
  // correspond to nodes are ignored, but if the number is greater than zero,
  // a warning message will be added to the return code.
  //
  // TODO: memoize / persist the node_to_id map so that we're not building it
  // every time.
  template <typename T>
  result<> set_node_column(const series_name& nodecol_name,
                           const T&           collection);

  /**
   * @brief Updates reverse node index after fresh edge ingestion.   Colelctive
   * method.
   *
   */
  void priv_update_reverse_node_index();

  /**
   * @brief Retrives or inserts node string label into reverse lookup.   Returns
   * local_node_idx
   *
   * @param label String node label
   * @return local_node_idx_type
   */
  local_node_idx_type priv_local_node_find_or_insert(std::string_view label);

  /**
   * @brief Retrives without inserting node string label into reverse lookup.
   * Returns local_node_idx
   *
   * @param label String node label
   * @return local_node_idx_type
   */
  std::optional<local_node_idx_type> priv_local_get_node_id(
    std::string_view label) const;

  /**
   * @brief Retrives node locator from reverse index.
   * If the locator is not found, that means the local data partition has no
   * knowledge of the node label.
   *
   *
   * @param label String node label
   * @return node_locator
   */
  std::optional<node_locator> priv_local_get_node_locator(
    std::string_view label) const;

  /**
   * @brief Check's the integrity of the indexes
   *
   * @return result<>
   */
  result<> priv_check_index_integrity() const;

  std::unordered_set<record_id_type> priv_random_idx(
    const std::unordered_set<record_id_type>& filtered_ids_set, size_t k,
    uint64_t seed);

  // Using YGM's default partitioner to assign node owner
  ygm::container::detail::hash_partitioner<
    ygm::container::detail::hash<std::string_view>>
    m_partitioner;

  template <typename T>
  result<> priv_set_column_by_idx(const series_name& col_name,
                                  const T&           collection);

  static count_types priv_series_to_count_type(
    const record_store_type::series_type& sv);

};  // class metall_graph

}  // namespace metalldata

template <>
struct std::hash<metalldata::metall_graph::series_types> {
  std::size_t operator()(
    const metalldata::metall_graph::series_types& v) const noexcept {
    std::size_t type_hash = hash<std::size_t>{}(v.index());
    std::size_t val_hash = std::visit(
      [](const auto& val) -> std::size_t {
        return hash<std::decay_t<decltype(val)>>{}(val);
      },
      v);
    return type_hash ^ (val_hash << 1);
  }
};

#include <metalldata/impl/metall_graph_locator.ipp>
#include <metalldata/impl/metall_graph_series_name.hpp>
#include <metalldata/impl/metall_graph_where.hpp>
#include <metalldata/impl/metall_graph_faker.ipp>
#include <metalldata/impl/metall_graph_priv_for_all.ipp>
#include <metalldata/impl/metall_graph_set_column.ipp>
#include <metalldata/impl/metall_graph_topk.ipp>
#include <metalldata/impl/metall_graph_series.ipp>