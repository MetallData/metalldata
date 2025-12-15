#pragma once

#include <boost/json.hpp>
#include <jsonlogic/logic.hpp>
#include <jsonlogic/src.hpp>
#include <iostream>
#include <fstream>
#include <metall/metall.hpp>
#include "multiseries/multiseries_record.hpp"

namespace bjsn = boost::json;

namespace {
using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

using persistent_string =
    boost::container::basic_string<char, std::char_traits<char>,
                                   metall::manager::allocator_type<char>>;

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
  std::vector<bjsn::string> vars;
  // jsonlogic::any_expr       expression_rule;

  auto jlrule = jsonlogic::create_logic(jl_rule);

  std::set<std::string> varset{};
  for (auto v : vars) {
    varset.emplace(v);
  }
  auto series = record_store.get_series_names();

  size_t fn_count = 0;
  record_store.for_all_dynamic(
      [jlexpr=std::move(jlrule), varset, series, &fn, &fn_count] (
          const record_store_type::record_id_type index,
          const auto&                             series_values) mutable {
        bjsn::object data{};
        bool         has_monostate = false;
        for (size_t i = 0; i < series.size(); ++i) {
          std::string s = series.at(i);
          auto        v = series_values.at(i);

          if (varset.contains(s)) {
            std::visit(
                [&data, &s, &has_monostate](auto&& v) {
                  using T = std::decay_t<decltype(v)>;
                  if constexpr (std::is_same_v<T, std::monostate>) {
                    has_monostate = true;
                  } else {
                    data[s] = boost::json::value(v);
                  }
                },
                v);
            if (has_monostate) {
              break;
            }
          }
        }
        if (has_monostate) {
          return;
        }

        bool res = truthy(jlexpr.apply(jsonlogic::json_accessor(data)));
        if (res) {
          fn(index, series_values);
          fn_count++;
        }
      });

  return fn_count;
}
