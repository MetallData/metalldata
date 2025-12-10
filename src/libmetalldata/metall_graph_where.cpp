// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <metall_jl/metall_jl.hpp>

namespace {
static auto priv_compile_jl_rule(bjsn::value jl_rule) {
  auto [expression_rule, vars_b, _] = jsonlogic::create_logic(jl_rule);

  std::vector<std::string> vars{vars_b.begin(), vars_b.end()};

  // Store the unique_ptr in a shared_ptr to make it copyable and shareable
  auto shared_expr =
    std::make_shared<jsonlogic::any_expr>(std::move(expression_rule));

  auto compiled =
    [shared_expr](
      const std::vector<metalldata::metall_graph::data_types>& row) -> bool {
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

}  // namespace
namespace metalldata {
metall_graph::where_clause::where_clause() {
  m_predicate = [](const std::vector<data_types>&) { return true; };
}

metall_graph::where_clause::where_clause(
  const std::vector<series_name>& s_names, pred_function pred)
    : m_series_names(s_names), m_predicate(pred) {};

metall_graph::where_clause::where_clause(
  const std::vector<std::string>& s_strnames, pred_function pred)
    : m_predicate(pred) {
  m_series_names.reserve(s_strnames.size());
  for (const auto& sn : s_strnames) {
    m_series_names.emplace_back(sn);
  }
}

metall_graph::where_clause::where_clause(const bjsn::value& jlrule) {
  auto [compiled, vars] = priv_compile_jl_rule(jlrule);

  m_predicate = compiled;
  m_series_names.reserve(vars.size());
  for (const auto& v : vars) {
    m_series_names.emplace_back(v);
  }
}

metall_graph::where_clause::where_clause(
  const std::string& jsonlogic_file_path) {
  bjsn::value jl   = jl::parseFile(jsonlogic_file_path);
  bjsn::value rule = jl.as_object()["rule"];

  auto [compiled, vars] = priv_compile_jl_rule(rule);
  m_predicate           = compiled;
  m_series_names.reserve(vars.size());
  for (const auto v : vars) {
    m_series_names.emplace_back(v);
  }
}

metall_graph::where_clause::where_clause(std::istream& jsonlogic_stream) {
  bjsn::value jl   = jl::parseStream(jsonlogic_stream);
  bjsn::value rule = jl.as_object()["rule"];

  auto [compiled, vars] = priv_compile_jl_rule(rule);
  m_predicate           = compiled;

  m_series_names.reserve(vars.size());
  for (const auto v : vars) {
    m_series_names.emplace_back(v);
  }
}

}  // namespace metalldata