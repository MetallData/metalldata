#pragma once

#include <boost/json.hpp>
#include <boost/json/src.hpp>
#include <jsonlogic/logic.hpp>
#include <jsonlogic/src.hpp>
#include <iostream>
#include <fstream>
#include "multiseries/multiseries_record.hpp"
#include "metall/utility/metall_mpi_adaptor.hpp"

namespace bjsn = boost::json;

namespace {
using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

using persistent_string =
    boost::container::basic_string<char, std::char_traits<char>,
                                   metall::manager::allocator_type<char>>;

using series_type = multiseries::record_store::series_type;
}  // namespace

namespace jl {
inline bjsn::value parseStream(std::istream& inps) {
  bjsn::stream_parser p;
  std::string         line;

  // \todo skips ws in strings
  while (inps >> line) {
    std::error_code ec;

    p.write(line.c_str(), line.size(), ec);

    if (ec) return nullptr;
  }

  std::error_code ec;
  p.finish(ec);
  if (ec) return nullptr;

  return p.release();
}

inline bjsn::value parseFile(const std::string& filename) {
  std::ifstream is{filename};

  return parseStream(is);
}
}  // namespace jl
template <typename Fn>
size_t apply_jl(bjsn::value jl_rule, record_store_type& record_store, Fn fn) {
  std::vector<bjsn::string> varnames;
  jsonlogic::any_expr       expression_rule;

  std::tie(expression_rule, varnames, std::ignore) =
      jsonlogic::create_logic(jl_rule);

  std::set<std::string> varset{};
  // create a map of series names to indices. Then create a vector of column ids
  // that correspond to varnames. Do this once, before for_all_dynamic. Then, in
  // for_all_dynamic, just grab the indices from series_values. This provides a
  // vector of variant values. f_a_d captures a vector of values to avoid
  // allocations.

  auto                           series = record_store.get_series_names();
  std::map<bjsn::string, size_t> series_idx;
  for (auto v : varnames) {
    for (size_t i = 0; i < series.size(); ++i) {
      if (v == series.at(i)) {
        series_idx[v] = i;
        break;
      }
    }
  }

  // var_idx holds a vector of series indices.
  std::vector<size_t> var_idx;
  var_idx.reserve(varnames.size());
  for (auto v : varnames) {
    var_idx.emplace_back(series_idx.at(v));
  }
  // varvalues holds the values for a given row index for all the variables.
  std::vector<series_type> varvalues;
  varvalues.reserve(var_idx.size());

  size_t fn_count = 0;
  record_store.for_all_dynamic(
      [var_idx, &varvalues, varset, series, &expression_rule, &fn, &fn_count](
          const record_store_type::record_id_type index,
          const auto&                             series_values) {
        if (series_values.empty()) {
          return;
        }
        for (size_t idx : var_idx) {
          series_type v = series_values.at(idx);
          using T       = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            return;
          }
          varvalues.emplace_back(series_values.at(idx));
        }

        if (varvalues.size() != var_idx.size()) {
          return;
        }
        jsonlogic::any_expr res_j =
            jsonlogic::apply(expression_rule, varvalues);

        auto res = jsonlogic::unpack_value<bool>(res_j);
        if (res) {
          fn(index);
          fn_count++;
        }
        varvalues.clear();
      });

  return fn_count;
}

template <typename Fn>
size_t apply_jl_series(const std::string& series_name, bjsn::value jl_rule,
                       record_store_type& record_store, Fn fn) {
  std::vector<bjsn::string> varnames;
  jsonlogic::any_expr       expression_rule;

  std::tie(expression_rule, varnames, std::ignore) =
      jsonlogic::create_logic(jl_rule);

  std::set<std::string> varset{};

  // create a map of series names to indices. Then create a vector of column ids
  // that correspond to varnames. Do this once, before for_all_dynamic. Then, in
  // for_all_dynamic, just grab the indices from series_values. This provides a
  // vector of variant values. f_a_d captures a vector of values to avoid
  // allocations.
  auto series = record_store.get_series_names();

  std::map<bjsn::string, size_t> series_idx;
  for (auto v : varnames) {
    for (size_t i = 0; i < series.size(); ++i) {
      if (v == series.at(i)) {
        series_idx[v] = i;
        break;
      }
    }
  }

  // get the index of the series_name. We probably should use find_series here
  // but it seems way too complex.
  // This current approach assumes that the series_name is valid.
  auto   it              = std::find(series.begin(), series.end(), series_name);
  size_t series_name_idx = std::distance(series.begin(), it);

  // var_idx holds a vector of series indices.
  std::vector<size_t> var_idx;
  var_idx.reserve(varnames.size());
  for (auto v : varnames) {
    var_idx.emplace_back(series_idx.at(v));
  }

  // varvalues holds the values for a given row index for all the variables.
  std::vector<series_type> varvalues;
  varvalues.reserve(var_idx.size());

  size_t fn_count = 0;
  record_store.for_all_dynamic(
      [series_name_idx, var_idx, &varvalues, varset, series, &expression_rule,
       &fn, &fn_count](const record_store_type::record_id_type index,
                       const auto&                             series_values) {
        if (series_values.empty()) {
          return;
        }

        for (size_t idx : var_idx) {
          series_type v = series_values.at(idx);
          using T       = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            return;
          }
          std::visit(
              [](const auto& x) {
                using T = std::decay_t<decltype(x)>;
                if constexpr (std::is_arithmetic_v<T> ||
                              std::is_same_v<T, std::string>) {
                }
              },
              v);

          varvalues.emplace_back(v);
        }

        if (varvalues.size() != var_idx.size()) {
          return;
        }
        jsonlogic::any_expr res_j =
            jsonlogic::apply(expression_rule, varvalues);

        auto res = jsonlogic::unpack_value<bool>(res_j);
        if (res) {
          series_type series_name_v = series_values.at(series_name_idx);
          fn(index, series_name_v);
          fn_count++;
        }
        varvalues.clear();
      });

  return fn_count;
}