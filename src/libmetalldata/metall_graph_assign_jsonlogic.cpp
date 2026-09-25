// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <array>
#include <cstdint>
#include <limits>
#include <metalldata/metall_graph.hpp>
#include "metall_graph_jl.hpp"

/*
 * assign_jsonlogic: create a series whose values are computed per row by a
 * jsonlogic expression.
 *
 * The type of the new series is not known up front, so this runs in two
 * passes over the rows selected by the where clause:
 *
 *   1. Probe: each rank evaluates rows until it gets its first non-null
 *      result and records that result's kind. The ranks then agree on one
 *      kind (bool < int64 < double widen; string only mixes with string).
 *   2. Write: the series is created with the agreed type and every selected
 *      row is evaluated again and stored. Results that widen losslessly to
 *      the series type are converted; anything else is left unset.
 *
 * Rows that produce no value (null / array result, missing input variable,
 * type mismatch) are left unset and reported as warnings in the result.
 */

namespace metalldata {

namespace {

/// Kind of value produced by an expression. Numeric kinds are ordered so
/// that the wider kind compares greater.
enum class value_kind : int {
  none = 0,
  boolean = 1,
  integer = 2,
  floating = 3,
  string = 4
};

std::string_view kind_name(value_kind k) {
  switch (k) {
    case value_kind::none:
      return "none";
    case value_kind::boolean:
      return "bool";
    case value_kind::integer:
      return "int64";
    case value_kind::floating:
      return "double";
    case value_kind::string:
      return "string";
  }
  return "unknown";
}

/// Warnings collected per rank and summed across ranks before returning.
enum warning_idx : size_t {
  w_missing_var = 0,
  w_eval_error,
  w_null_result,
  w_array_result,
  w_uint_range,
  w_mismatch_to_bool,
  w_mismatch_to_int,
  w_mismatch_to_double,
  w_mismatch_to_string,
  w_count
};

constexpr std::array<std::string_view, w_count> warning_msgs = {
  "row skipped: an input variable is missing",
  "row skipped: expression raised an error",
  "row skipped: expression returned null",
  "row skipped: expression returned an array",
  "row skipped: unsigned result does not fit in int64",
  "row skipped: result could not be stored in a bool series",
  "row skipped: result could not be stored in an int64 series",
  "row skipped: result could not be stored in a double series",
  "row skipped: result could not be stored in a string series",
};

using owned_value = metall_graph::data_types;

/// Converts a jsonlogic result to an owned value. Strings are copied because
/// the view may be invalidated by the next evaluation.
owned_value to_owned(const jsonlogic::value_variant&    v,
                     std::array<size_t, w_count>& warnings) {
  return std::visit(
    [&](const auto& x) -> owned_value {
      using T = std::decay_t<decltype(x)>;
      if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, int64_t> ||
                    std::is_same_v<T, double>) {
        return x;
      } else if constexpr (std::is_same_v<T, uint64_t>) {
        if (x > uint64_t(std::numeric_limits<int64_t>::max())) {
          ++warnings[w_uint_range];
          return std::monostate{};
        }
        return int64_t(x);
      } else if constexpr (std::is_same_v<T, jsonlogic::managed_string_view>) {
        return std::string(x.view());
      } else if constexpr (std::is_same_v<T, jsonlogic::array_value const*>) {
        ++warnings[w_array_result];
        return std::monostate{};
      } else {  // std::monostate, std::nullptr_t
        ++warnings[w_null_result];
        return std::monostate{};
      }
    },
    static_cast<const jsonlogic::value_variant_base&>(v));
}

value_kind kind_of(const owned_value& v) {
  return std::visit(
    [](const auto& x) {
      using T = std::decay_t<decltype(x)>;
      if constexpr (std::is_same_v<T, bool>) {
        return value_kind::boolean;
      } else if constexpr (std::is_same_v<T, int64_t>) {
        return value_kind::integer;
      } else if constexpr (std::is_same_v<T, double>) {
        return value_kind::floating;
      } else if constexpr (std::is_same_v<T, std::string>) {
        return value_kind::string;
      } else {
        return value_kind::none;
      }
    },
    v);
}

/// Stores `v` into the series using `setter`, widening bool->int64 and
/// bool/int64->double. Returns false if `v` cannot be represented as T.
template <typename T, typename Setter>
bool store_as(const owned_value& v, Setter&& setter) {
  return std::visit(
    [&](const auto& x) -> bool {
      using V = std::decay_t<decltype(x)>;
      if constexpr (std::is_same_v<T, std::string_view>) {
        if constexpr (std::is_same_v<V, std::string>) {
          setter(std::string_view(x));
          return true;
        }
      } else if constexpr (std::is_same_v<V, T>) {
        setter(x);
        return true;
      } else if constexpr (std::is_same_v<T, int64_t> &&
                           std::is_same_v<V, bool>) {
        setter(int64_t(x));
        return true;
      } else if constexpr (std::is_same_v<T, double> &&
                           (std::is_same_v<V, bool> ||
                            std::is_same_v<V, int64_t>)) {
        setter(double(x));
        return true;
      }
      return false;
    },
    v);
}

}  // namespace

result<> metall_graph::assign_jsonlogic(
  series_name name, const bjsn::value& jl_rule,
  const metall_graph::where_clause& where) {
  result<> to_return;

  if (!name.is_node_series() && !name.is_edge_series()) {
    return std::unexpected(
      std::format("unknown series name: {}", name.qualified()));
  }
  if (has_series(name)) {
    return std::unexpected(
      std::format("series {} already exists", name.qualified()));
  }

  detail::compiled_jl_expr expr;
  try {
    expr = detail::compile_jl_expr(jl_rule);
  } catch (const std::exception& e) {
    return std::unexpected(
      std::format("invalid jsonlogic expression: {}", e.what()));
  }
  if (expr.has_computed_vars) {
    return std::unexpected(
      "jsonlogic expression uses computed variable names, which are not "
      "supported");
  }

  // Variables must live in the same table as the target series.
  auto* store = name.is_node_series() ? m_pnodes : m_pedges;
  std::vector<series_index_type> var_idxs;
  var_idxs.reserve(expr.vars.size());
  for (const auto& v : expr.vars) {
    series_name vname(v);
    if (vname.prefix() != name.prefix()) {
      return std::unexpected(std::format(
        "variable {} is not a {} series; the expression may only reference "
        "series in the same table as {}",
        v, name.prefix(), name.qualified()));
    }
    auto idx_o = store->find_series(vname.unqualified());
    if (!idx_o.has_value()) {
      return std::unexpected(std::format("series {} not found", v));
    }
    var_idxs.push_back(idx_o.value());
  }

  std::array<size_t, w_count> local_warnings{};

  // Evaluates the expression on one local row. Returns monostate if the row
  // produces no value (a warning is recorded when appropriate).
  std::vector<series_types> row;
  row.reserve(var_idxs.size());
  auto eval = [&](size_t row_index) -> owned_value {
    row.clear();
    for (auto sidx : var_idxs) {
      if (store->is_none(sidx, row_index)) {
        ++local_warnings[w_missing_var];
        return std::monostate{};
      }
      row.push_back(store->get_dynamic(sidx, row_index).value());
    }
    // jsonlogic throws on some inputs (e.g. "red" + 1). The row loops are
    // collective, so an exception escaping on one rank would leave the other
    // ranks waiting in a barrier; skip the row instead.
    try {
      return to_owned(expr.fn(row), local_warnings);
    } catch (...) {
      ++local_warnings[w_eval_error];
      return std::monostate{};
    }
  };

  // Runs `fn(row_index)` for every local row matching the where clause.
  auto for_all_matching = [&](auto fn) {
    if (name.is_node_series()) {
      priv_for_all_nodes(
        [&](local_node_idx_type nid) { fn(std::to_underlying(nid)); }, where);
    } else {
      priv_for_all_edges(
        [&](local_edge_idx_type eid) { fn(std::to_underlying(eid)); }, where);
    }
  };

  // Pass 1: find the kind of the first non-null result on this rank. The
  // probe's warnings are discarded; pass 2 re-evaluates these rows.
  value_kind local_kind = value_kind::none;
  for_all_matching([&](size_t row_index) {
    if (local_kind == value_kind::none) {
      local_kind = kind_of(eval(row_index));
    }
  });
  local_warnings.fill(0);

  // Agree on one kind across ranks.
  bool any_string = ygm::logical_or(local_kind == value_kind::string, m_comm);
  int  max_numeric = ygm::max(
    local_kind == value_kind::string ? 0 : std::to_underlying(local_kind),
    m_comm);
  if (any_string && max_numeric > 0) {
    return std::unexpected(std::format(
      "expression produces both string and {} values; cannot choose a type "
      "for {}",
      kind_name(value_kind(max_numeric)), name.qualified()));
  }
  value_kind kind = any_string ? value_kind::string : value_kind(max_numeric);
  if (kind == value_kind::none) {
    return std::unexpected(std::format(
      "expression produced no values; {} was not created", name.qualified()));
  }

  // Pass 2: create the series and write the values.
  auto write_all = [&]<typename T>(warning_idx mismatch_warning) {
    auto sidx = store->template add_series<T>(name.unqualified());
    for_all_matching([&](size_t row_index) {
      auto v = eval(row_index);
      if (std::holds_alternative<std::monostate>(v)) {
        return;
      }
      bool stored = store_as<T>(
        v, [&](const auto& x) { store->set(sidx, row_index, x); });
      if (!stored) {
        ++local_warnings[mismatch_warning];
      }
    });
  };

  switch (kind) {
    case value_kind::boolean:
      write_all.template operator()<bool>(w_mismatch_to_bool);
      break;
    case value_kind::integer:
      write_all.template operator()<int64_t>(w_mismatch_to_int);
      break;
    case value_kind::floating:
      write_all.template operator()<double>(w_mismatch_to_double);
      break;
    case value_kind::string:
      write_all.template operator()<std::string_view>(w_mismatch_to_string);
      break;
    case value_kind::none:
      break;  // unreachable, handled above
  }

  // Report global warning counts on every rank.
  for (size_t i = 0; i < w_count; ++i) {
    size_t n = ygm::sum(local_warnings[i], m_comm);
    if (n > 0) {
      to_return.add_warnings(n, std::string(warning_msgs[i]));
    }
  }

  return to_return;
}

}  // namespace metalldata
