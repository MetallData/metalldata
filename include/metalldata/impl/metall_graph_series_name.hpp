// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <dirent.h>
#include <metalldata/metall_graph.hpp>

namespace metalldata {

struct metall_graph::series_name {
 public:
  series_name() = default;
  explicit series_name(std::string_view name) {
    auto [prefix, unqualified] = priv_split_series_str(name);
    m_prefix = prefix;
    m_unqualified = unqualified;
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

  bool operator==(std::string_view other) const { return qualified() == other; }

  // required to make collections / sets of series_names
  bool operator<(const series_name& other) const;

  bool is_reserved() const {
    return *this == U_COL || *this == V_COL || *this == DIR_COL ||
           *this == NODE_COL;
  }

  // TODO:  Delete these after updating reference locations.  These are "full
  // qualified" series names.
  static const series_name U_COL;
  static const series_name V_COL;
  static const series_name DIR_COL;
  static const series_name NODE_COL;

 private:
  std::string m_prefix;
  std::string m_unqualified;

  static std::pair<std::string_view, std::string_view> priv_split_series_str(
    std::string_view str);
};  // series_name

inline const metall_graph::series_name metall_graph::series_name::U_COL{
  "edge.u"};
inline const metall_graph::series_name metall_graph::series_name::V_COL{
  "edge.v"};
inline const metall_graph::series_name metall_graph::series_name::DIR_COL{
  "edge.directed"};
inline const metall_graph::series_name metall_graph::series_name::NODE_COL{
  "node.id"};

}  // namespace metalldata

// Specialize std::hash for series_name to enable use in unordered containers
template <>
struct std::hash<metalldata::metall_graph::series_name> {
  std::size_t operator()(
    const metalldata::metall_graph::series_name& sn) const {
    return std::hash<std::string>{}(sn.qualified());
  }
};