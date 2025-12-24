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
#include <string>
#include <string_view>
#include <vector>
#include <metall/metall.hpp>
#include <multiseries/multiseries_record.hpp>
#include <ygm/comm.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <boost/json.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <ygm/container/set.hpp>
#include <expected>
#include <optional>

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
  using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
  using string_store_type = record_store_type::string_store_type;

  using record_id_type = record_store_type::record_id_type;

 public:
  // TODO: Rationalize these data types to correspond better with JSONLogic and
  // MetallFrame.
  // TODO: we need unsigned ints here anyway.
  using data_types =
    std::variant<size_t, double, bool, std::string, std::monostate>;

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

  struct series_name {
   public:
    series_name() = default;
    series_name(std::string_view name) {
      auto [prefix, unqualified] = priv_split_series_str(name);
      m_prefix                   = prefix;
      m_unqualified              = unqualified;
    };

    series_name(std::string_view prefix, std::string_view unqualified)
        : m_prefix(prefix), m_unqualified(unqualified) {};

    bool empty() const { return m_prefix.empty() && m_unqualified.empty(); }
    bool is_node_series() const { return m_prefix == "node"; }
    bool is_edge_series() const { return m_prefix == "edge"; }

    bool is_qualified() const { return !m_prefix.empty(); }

    std::string_view prefix() const { return m_prefix; }
    std::string_view unqualified() const { return m_unqualified; }
    std::string      qualified() const {
      if (!is_qualified()) {
        return m_unqualified;
      }
      return m_prefix + "." + m_unqualified;
    }

    friend std::ostream& operator<<(std::ostream& os, const series_name& obj) {
      if (obj.is_qualified()) {
        os << obj.m_prefix << ".";
      }
      os << obj.m_unqualified;
      return os;
    }

    bool operator==(const series_name& other) const {
      return m_prefix == other.m_prefix && m_unqualified == other.m_unqualified;
    }

    bool operator==(std::string_view other) const {
      return qualified() == other;
    }

    // required to make collections / sets of series_names
    bool operator<(const series_name& other) const {
      if (m_prefix != other.m_prefix) {
        return m_prefix < other.m_prefix;
      }
      return m_unqualified < other.m_unqualified;
    }

   private:
    std::string m_prefix;
    std::string m_unqualified;

    static std::pair<std::string_view, std::string_view> priv_split_series_str(
      std::string_view str) {
      std::string_view prefix;
      std::string_view unqualified;
      size_t           pos = str.find('.');
      if (pos != std::string_view::npos) {
        prefix      = str.substr(0, pos);
        unqualified = str.substr(pos + 1);
      } else {
        prefix      = std::string_view{};
        unqualified = str;
      }
      return std::make_pair(prefix, unqualified);
    }
  };  // series_name

  /// if the where_clause is default constructed, m_has_predicate is false,
  /// which means:
  // 1) is_node_clause and is_edge_clause are both true
  // 2) evaluate() will always return true
  // TODO: get rid of node and edge differentiation.
  // TODO: get rid of initialized
  struct where_clause {
   private:
    using pred_function = std::function<bool(const std::vector<data_types>&)>;

   public:
    where_clause();

    where_clause(const std::vector<std::string>&                     s_names,
                 std::function<bool(const std::vector<data_types>&)> pred);

    where_clause(const std::vector<series_name>& s_names, pred_function pred);

    where_clause(const bjsn::value& jlrule);

    where_clause(const bjsn::object& obj);

    where_clause(const std::string& jsonlogic_file_path);

    where_clause(std::istream& jsonlogic_stream);

    const std::vector<series_name>& series_names() const {
      return m_series_names;
    }

    bool good() const {
      if (m_series_names.empty()) {
        return true;
      }

      auto first_series_name = m_series_names.front();
      auto first_prefix      = first_series_name.prefix();

      for (const auto& name : m_series_names) {
        if (first_prefix != name.prefix()) {
          return false;
        }
      }
      return true;
    }

    bool is_node_clause() const {
      return !m_series_names.empty() &&
             m_series_names.front().is_node_series() && good();
    }

    bool is_edge_clause() const {
      return !m_series_names.empty() &&
             m_series_names.front().is_edge_series() && good();
    }

    const auto& predicate() const { return m_predicate; }

    bool evaluate(const std::vector<data_types>& data) const {
      if (m_series_names.empty()) {
        return true;
      }
      return m_predicate(data);
    }

    bool empty() const { return m_series_names.empty(); }

   private:
    std::vector<series_name>                            m_series_names;
    std::function<bool(const std::vector<data_types>&)> m_predicate;
  };  // where_clause

  /*

    where = node.age > 21 && node.zipcode = 77845

    where = edge.u.age > edge.v.age

    unsupported =  (node.age > 21 && node.zipcode = 77845) & (edge.u.age >
    edge.v.age)


  */

  // these are "full qualified" selectors.
  const series_name           U_COL    = series_name("edge.u");
  const series_name           V_COL    = series_name("edge.v");
  const series_name           DIR_COL  = series_name("edge.directed");
  const series_name           NODE_COL = series_name("node.id");
  const std::set<series_name> RESERVED_COLUMN_NAMES = {DIR_COL, U_COL, V_COL};

  metall_graph(ygm::comm& comm, std::string_view path, bool overwrite = false);

  ~metall_graph();

  //
  // Ingest from parquet, provide 2 column names to define an edge, provide if
  // directed, provide list of optional metadata fields
  return_code ingest_parquet_edges(
    std::string_view path, bool recursive, std::string_view col_u,
    std::string_view col_v, bool directed,
    const std::optional<std::vector<series_name>>& meta = std::nullopt);

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

  return_code dump_parquet_verts(std::string_view                path,
                                 const std::vector<series_name>& meta,
                                 bool overwrite = false);

  return_code dump_parquet_edges(std::string_view                path,
                                 const std::vector<series_name>& meta,
                                 bool overwrite = false);

  return_code erase_edges(const where_clause& where = where_clause{});

  return_code erase_edges(const series_name&                     name,
                          boost::unordered_flat_set<std::string> haystack);

  template <typename Fn, typename T>  // defined in metall_graph_faker.hpp
  return_code add_faker_series(const metall_graph::series_name& name,
                               Fn                               faker_func,
                               const where_clause& where = where_clause{});

  template <typename T, typename Compare = std::greater<T>>
  std::vector<data_types> topk(size_t k, const series_name& ser_name,
                               const std::vector<series_name>& ser_inc,
                               Compare                         comp = Compare(),
                               const where_clause& where = where_clause());

  std::map<std::string, std::string> get_edge_selector_info() {
    // Since the m_pedges schema is identical across ranks, we don't have to
    // collect. Also: the "edge" prefix (and "node" in the corresponding
    // function) need to match the corresponding meta.json values.
    std::map<std::string, std::string> sels;
    for (const auto& el : m_pedges->get_series_names()) {
      auto sel  = std::format("edge.{}", el);
      sels[sel] = "default";
    }
    for (const auto& el : m_pnodes->get_series_names()) {
      auto sel  = std::format("{}.{}", U_COL.qualified(), el);
      sels[sel] = "inherited";
      sel       = std::format("{}.{}", V_COL.qualified(), el);
      sels[sel] = "inherited";
    }

    return sels;
  }

  std::map<std::string, std::string> get_node_selector_info() {
    // Since the m_pedges schema is identical across ranks, we don't have to
    // collect.
    std::map<std::string, std::string> sels;
    for (const auto& el : m_pnodes->get_series_names()) {
      auto sel  = std::format("node.{}", el);
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

  // TODO: right now, if adding a series of type string, T must be string_view
  // in order to satisfy the demands of the underlying multiseries_record.
  // However, this should really be std::string since that's the variant type
  // that metallgraph uses. We should allow T to be std::string and "convert"
  // as necessary behind the scenes.
  template <typename T>
  bool add_series(series_name name) {  // "node.color" or "edge.time"
    if (has_series(name)) {
      return false;
    }
    if (name.is_node_series()) {
      m_pnodes->add_series<T>(name.unqualified());
      return true;
    }
    if (name.is_edge_series()) {
      m_pedges->add_series<T>(name.unqualified());
      return true;
    }
    return false;
  }

  template <typename T>
  bool add_series(std::string_view name) {
    series_name sname{name};
    return add_series<T>(sname);
  }

  // drop_series requires a qualified selector name (starts with node. or
  // edge.)
  bool drop_series(const series_name& name);

  // has_node_series requires an UNqualified (stripped) selector name.
  bool has_node_series(std::string_view unqualified_name) const {
    return m_pnodes->contains_series(unqualified_name);
  };

  bool has_node_series(series_name name) const {
    return name.is_node_series() &&
           m_pnodes->contains_series(name.unqualified());
  }
  bool has_edge_series(std::string_view unqualified_name) const {
    return m_pedges->contains_series(unqualified_name);
  };

  bool has_edge_series(series_name name) const {
    return name.is_edge_series() &&
           m_pedges->contains_series(name.unqualified());
  }

  bool has_series(series_name name) const {
    return has_edge_series(name) || has_node_series(name);
  }
  // has_series requires a qualified selector name (starting with node or
  // edge)
  bool has_series(std::string_view name) const {
    return has_series(series_name(name));
  };

  std::vector<series_name> get_node_series_names() const {
    std::vector<series_name> sns;
    for (auto n : m_pnodes->get_series_names()) {
      sns.emplace_back(series_name("node", n));
    }
    return sns;
  };

  std::vector<series_name> get_edge_series_names() const {
    std::vector<series_name> sns;
    for (auto n : m_pedges->get_series_names()) {
      sns.emplace_back(series_name("edge", n));
    }
    return sns;
  };

  size_t num_edges(const where_clause& where = where_clause{}) const {
    size_t local_size = local_num_edges();
    if (!where.empty()) {
      local_size = 0;
      priv_for_all_edges([&](auto) { ++local_size; }, where);
    }
    return ygm::sum(local_size, m_comm);
  }

  size_t num_nodes(const where_clause& where = where_clause{}) const {
    size_t local_size = local_num_nodes();
    if (!where.empty()) {
      local_size = 0;
      priv_for_all_nodes([&](auto) { ++local_size; }, where);
    }
    return ygm::sum(local_size, m_comm);
  }

  size_t num_node_series() const { return m_pnodes->num_series(); };

  size_t num_edge_series() const { return m_pedges->num_series(); };

  template <typename Fn>
  void visit_node_field(series_name name, size_t record_id, Fn func) const {
    assert(name.is_node_series());
    m_pnodes->visit_field(name.unqualified(), record_id, func);
  }

  template <typename Fn>
  void visit_edge_field(series_name name, size_t record_id, Fn func) const {
    assert(name.is_edge_series());
    m_pedges->visit_field(name.unqualified(), record_id, func);
  }

  std::expected<boost::json::array, std::string> select_edges(
    const std::unordered_set<metall_graph::series_name>& series_set,
    const metall_graph::where_clause& where, size_t limit);

  std::expected<boost::json::array, std::string> select_nodes(
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

  return_code in_degree(series_name out_name,
                        const where_clause& = where_clause());

  return_code out_degree(series_name out_name,
                         const where_clause& = where_clause());

  return_code degrees(series_name in_name, series_name out_name,
                      const where_clause& = where_clause());

  return_code degrees2(series_name in_name, series_name out_name,
                       const where_clause& = where_clause());

  return_code connected_components(series_name out_name,
                                   const where_clause& = where_clause());

  // struct ego_net_options {
  //   std::optional<std::string> v_dist_closest;
  //   std::optional<std::string> v_closest_source;
  //   std::optional<std::string> e_included;
  //   std::optional<std::string> v_included;
  // };

  return_code nhops(series_name out_node_series, size_t nhops,
                    std::vector<std::string> sources,
                    const where_clause&      where = where_clause());

  // TODO: also allow val a function
  return_code assign(series_name series_name, const data_types& val,
                     const where_clause& = where_clause());

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

  return_code priv_in_out_degree(series_name name, const where_clause&,
                                 bool        outdeg);

  template <typename Fn>
  void priv_for_all_edges(Fn                  func,
                          const where_clause& where = where_clause()) const;

  template <typename Fn>
  void priv_for_all_nodes(Fn                  func,
                          const where_clause& where = where_clause()) const;

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
  return_code set_node_column(series_name nodecol_name, const T& collection);

  /// \brief Convert a multiseries::series_type to a metall_graph::data_types
  static data_types convert_to_data_type(
    const record_store_type::series_type& val) {
    return std::visit(
      [](const auto& v) -> data_types {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return std::monostate{};
        } else if constexpr (std::is_same_v<T, bool>) {
          return v;
        } else if constexpr (std::is_same_v<T, double>) {
          return v;
        } else if constexpr (std::is_same_v<T, int64_t> ||
                             std::is_same_v<T, uint64_t>) {
          return static_cast<size_t>(v);
        } else if constexpr (std::is_same_v<T, std::string_view>) {
          return std::string(v);
        }
      },
      val);
  }

};  // class metall_graph

}  // namespace metalldata

// Specialize std::hash for series_name to enable use in unordered containers
namespace std {
template <>
struct hash<metalldata::metall_graph::series_name> {
  std::size_t operator()(
    const metalldata::metall_graph::series_name& sn) const {
    return std::hash<std::string>{}(sn.qualified());
  }
};
}  // namespace std

#include <metalldata/impl/metall_graph_faker.ipp>
#include <metalldata/impl/metall_graph_priv_for_all.ipp>
#include <metalldata/impl/metall_graph_set_node_column.ipp>
#include <metalldata/impl/metall_graph_topk.ipp>