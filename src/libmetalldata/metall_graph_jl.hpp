// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// Private helper for compiling a jsonlogic expression against metall_graph
// rows. Used by where_clause (which tests the result for truthiness) and by
// assign_jsonlogic (which stores the result).
//
// NOTE: only include <jsonlogic/logic.hpp> (declarations) here. The jsonlogic
// implementation (<jsonlogic/src.hpp>, via metall_jl.hpp) has non-inline
// definitions and is compiled exactly once, in metall_graph_where.cpp, which
// is also where compile_jl_expr is defined.

#pragma once

#include <functional>
#include <string>
#include <vector>
#include <jsonlogic/logic.hpp>
#include <metalldata/metall_graph.hpp>

namespace metalldata::detail {

/// A compiled jsonlogic expression. `fn` takes the row values for `vars` (in
/// the same order) and returns the raw jsonlogic result.
///
/// NOTE: a string result is a view that may refer to storage owned by the
/// rule; it is only valid until the next call to `fn`.
struct compiled_jl_expr {
  std::function<jsonlogic::value_variant(
    const std::vector<metall_graph::series_types>&)>
                           fn;
  std::vector<std::string> vars;
  bool                     has_computed_vars = false;
};

/// Compiles `jl_rule`. Throws if the rule is not valid jsonlogic.
compiled_jl_expr compile_jl_expr(const bjsn::value& jl_rule);

}  // namespace metalldata::detail
