// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief This file contains common code for the metall_frame implementation

#pragma once

#include <cassert>
#include <limits>
#include <string>

#include <metall/metall.hpp>

#include <ygm/comm.hpp>

#include "experimental/cxx-compat.hpp"

#define WITH_YGM 1

#include <clippy/clippy-eval.hpp>
#include <clippy/clippy.hpp>

#include "metall_frame.hpp"


using JsonExpression = std::vector<boost::json::object>;

// \todo should we use JsonExpression also to describe the columns?
using ColumnSelector = std::vector<std::string>;

namespace {
const std::string MF_CLASS_NAME           = "MetallFrame";
const std::string ST_METALL_LOCATION_NAME = "metall_location";
const std::string ST_METALL_LOCATION_DESC = "The metall location.";
const std::string ST_METALLFRAME_NAME     = "dataframe_key";
const std::string ST_METALLFRAME_DESC     = "The name of the MetallFrame in Metall. (default: \"_default\")";
const std::string ST_METALLFRAME_DFLT     = "_default";
const std::string ST_SELECTED             = "selected";
const std::string KEYS_SELECTOR           = "keys";

CXX_MAYBE_UNUSED
inline void append(std::vector<boost::json::object>& lhs,
                   std::vector<boost::json::object>  rhs) {
  if (lhs.size() == 0) return lhs.swap(rhs);

  std::move(rhs.begin(), rhs.end(), std::back_inserter(lhs));
}


//
// common convenience functions
#if 0

inline json_logic::ValueExpr toValueExpr(experimental::dataframe_variant_t el) {
  if (const experimental::string_t* s =
          std::get_if<experimental::string_t>(&el))
    return json_logic::toValueExpr(boost::json::string(s->begin(), s->end()));

  if (const experimental::int_t* i = std::get_if<experimental::int_t>(&el))
    return json_logic::toValueExpr(*i);

  if (const experimental::real_t* r = std::get_if<experimental::real_t>(&el))
    return json_logic::toValueExpr(*r);

  if (const experimental::uint_t* u = std::get_if<experimental::uint_t>(&el))
    return json_logic::toValueExpr(*u);

  CXX_UNLIKELY;
  return json_logic::toValueExpr(nullptr);
}

inline std::vector<int> generateIndexN(std::vector<int> v, int count) {
  v.reserve(count);
  std::generate_n(std::back_inserter(v), count,
                  [i = -1]() mutable -> int { return ++i; });

  return v;
}

/// Calls \fn(row, \dataset[row]) for all rows of \dataset, where all
/// \predicates hold. \param Fn         a functor that is called with an integer
/// and a row of \dataset. \param dataset    the data store \param predicates a
/// vector of JSON expressions that are evaluated for each row \param numrows
/// max number times that fn is called on a given rank \details
///   the order of calls to \fn is unspecified.
template <class Fn, class DataSequence>
inline void forAllSelected(Fn fn, int rank, DataSequence& dataset,
                           JsonExpression predicates = {},
                           int numrows = std::numeric_limits<int>::max()) {
  std::vector<json_logic::AnyExpr> queries;

  // prepare AST
  for (boost::json::object& jexp : predicates) {
    auto [ast, vars, hasComputedVarNames] =
        json_logic::translateNode(jexp["rule"]);

    if (hasComputedVarNames)
      throw std::runtime_error("unable to work with computed variable names");

    // check that all free variables are prefixed with SELECTED
    for (const boost::json::string& varname : vars) {
      if (varname.rfind(SELECTOR, 0) != 0)
        throw std::logic_error("unknown selector");
      if (varname.find('.') != SELECTOR.size())
        throw std::logic_error("unknown selector.");
    }

    queries.emplace_back(std::move(ast));
  }

  std::vector<int> selectedRows;
  // for (const auto& row : dataset)
  for (std::int64_t row = 0, zzz = dataset.rows(); row < zzz; ++row) {
    const std::int64_t selLen    = (SELECTOR.size() + 1);
    auto               varLookup = [&dataset, selLen, row, rank](
                         const boost::json::string& colname,
                         int) -> json_logic::ValueExpr {
      // \todo match selector instead of skipping it
      std::string_view col{colname.begin() + selLen, colname.size() - selLen};

      try {
        return toValueExpr(dataset.get_cell_variant(row, col));
      } catch (const experimental::unknown_column_error&) {
        if (col == "rowid") return json_logic::toValueExpr(row);
        if (col == "mpiid") return json_logic::toValueExpr(std::int64_t(rank));

        return json_logic::toValueExpr(nullptr);
      }
    };

    auto rowPredicate = [varLookup](json_logic::AnyExpr& query) -> bool {
      json_logic::ValueExpr exp = json_logic::calculate(query, varLookup);

      return !json_logic::unpackValue<bool>(std::move(exp));
    };

    const std::vector<json_logic::AnyExpr>::iterator lim = queries.end();
    const std::vector<json_logic::AnyExpr>::iterator pos =
        std::find_if(queries.begin(), lim, rowPredicate);

    if (pos == lim) {
      //~ fn(rownum, row);
      fn(row);

      if (0 == --numrows) {
        CXX_UNLIKELY;
        break;
      }
    }
  }
}

std::vector<int> computeSelected(
    int rank, const vector_json_type& dataset, JsonExpression jsonExpression,
    int numrows = std::numeric_limits<int>::max()) {
  std::vector<int> res;

  forAllSelected([&res](int rownum) -> void { res.push_back(rownum); }, rank,
                 dataset, std::move(jsonExpression), numrows);

  return res;
}

inline std::vector<int> getSelectedRows(
    int rank, const clippy::clippy& clip, const vector_json_type& vec,
    int numrows = std::numeric_limits<int>::max()) {
  if (!clip.has_state(ST_SELECTED)) {
    CXX_UNLIKELY;
    return generateIndexN(std::vector<int>{},
                          std::min(numrows, int(vec.rows())));
  }

  JsonExpression jsonExpression = clip.get_state<JsonExpression>(ST_SELECTED);

  return computeSelected(rank, vec, std::move(jsonExpression), numrows);
}

#if NOT_PORTED_YET
template <class JsonObject>
auto if_contains(const JsonObject& obj, const std::string& name)
    -> decltype(&obj.at(name)) {
  auto pos = obj.find(name);

  return (pos == obj.end()) ? nullptr : &pos->value();
}
#endif /* NOT_PORTED_YET */


#endif

}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv);

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  return ygm_main(world, argc, argv);
}
