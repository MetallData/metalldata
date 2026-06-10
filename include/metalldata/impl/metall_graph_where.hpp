#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
struct metall_graph::where_clause {
 private:
  using pred_function =
    std::function<bool(const std::vector<metall_graph::series_types>&)>;

 public:
  where_clause();

  where_clause(
    const std::vector<std::string>&                                     s_names,
    std::function<bool(const std::vector<metall_graph::series_types>&)> pred);

  where_clause(const std::vector<metall_graph::series_name>& s_names,
               pred_function                                 pred);

  where_clause(const bjsn::value& jlrule);

  where_clause(const bjsn::object& obj);

  where_clause(const std::string& jsonlogic_file_path);

  where_clause(std::istream& jsonlogic_stream);

  const std::vector<metall_graph::series_name>& series_names() const;

  bool good() const;

  bool is_node_clause() const;

  bool is_edge_clause() const;

  const auto& predicate() const;

  bool evaluate(const std::vector<metall_graph::series_types>& data) const;

  bool empty() const;

 private:
  std::vector<metall_graph::series_name> m_series_names;
  std::function<bool(const std::vector<metall_graph::series_types>&)>
    m_predicate;
};  // where_clause
}  // namespace metalldata