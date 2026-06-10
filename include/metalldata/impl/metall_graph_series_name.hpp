#pragma once
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
      prefix = str.substr(0, pos);
      unqualified = str.substr(pos + 1);
    } else {
      prefix = std::string_view{};
      unqualified = str;
    }
    return std::make_pair(prefix, unqualified);
  }
};  // series_name

namespace detail {
// TODO:  Delete these after updating reference locations.  These are "full
// qualified" series names.
static const metall_graph::series_name U_COL{"edge.u"};
static const metall_graph::series_name V_COL{"edge.v"};
static const metall_graph::series_name DIR_COL{"edge.directed"};
static const metall_graph::series_name NODE_COL{"node.id"};
// TODO:  Replace with an priv_is_reserved_name() method
static const std::set<metall_graph::series_name> RESERVED_COLUMN_NAMES{
  {DIR_COL, U_COL, V_COL}};
}  // namespace detail

}  // namespace metalldata

// Specialize std::hash for series_name to enable use in unordered containers
template <>
struct std::hash<metalldata::metall_graph::series_name> {
  std::size_t operator()(
    const metalldata::metall_graph::series_name& sn) const {
    return std::hash<std::string>{}(sn.qualified());
  }
};