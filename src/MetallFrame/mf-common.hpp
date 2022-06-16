// Copyright 2022 Lawrence Livermore National Security, LLC and other CLIPPy Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief This file contains common code for the JsonFrame implementation

#pragma once

#include <string>
#include <limits>

#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <metall/container/vector.hpp>
#include <metall/container/experimental/json/json.hpp>

#include <ygm/comm.hpp>

#include <experimental/cxx-compat.hpp>
#include <clippy/clippy.hpp>
#include <clippy/clippy-eval.hpp>


using json_value_type = metall::container::experimental::json::value<metall::manager::allocator_type<std::byte>>;
using vector_json_type = metall::container::vector<json_value_type, metall::manager::scoped_allocator_type<json_value_type>>;

using JsonExpression   = std::vector<boost::json::object>;

// \todo should we use JsonExpression also to describe the columns?
using ColumnSelector = std::vector<std::string>;

namespace
{
  const std::string CLASS_NAME = "JsonFrame";
  const std::string ST_METALL_LOCATION = "metall_location";
  const std::string ST_SELECTED = "selected";
  const std::string SELECTOR = "keys";

  inline
  vector_json_type&
  jsonVector(metall::utility::metall_mpi_adaptor& mgr)
  {
    vector_json_type* res = mgr.get_local_manager().find<vector_json_type>(metall::unique_instance).first;

    if (res == nullptr)
    {
      CXX_UNLIKELY;
      throw std::runtime_error("Unable to open JsonFrame");
    }

    return *res;
  }

  json_logic::ValueExpr
  toValueExpr(const json_value_type& el)
  {
    if (el.is_int64())  return json_logic::toValueExpr(el.as_int64());
    if (el.is_uint64()) return json_logic::toValueExpr(el.as_uint64());
    if (el.is_double()) return json_logic::toValueExpr(el.as_double());
    if (el.is_null())   return json_logic::toValueExpr(nullptr);

    assert(el.is_string()); // \todo array

    const auto& str = el.as_string();

    return json_logic::toValueExpr(boost::json::string(str.begin(), str.end()));
  }

  std::vector<int>
  computeSelected(const vector_json_type& dataset, JsonExpression& jsonExpression, int numrows = std::numeric_limits<int>::max())
  {
    std::vector<int> res;
    std::vector<json_logic::AnyExpr> queries;

    // prepare AST
    for (boost::json::object& jexp : jsonExpression)
    {
      auto [ast, vars, hasComputedVarNames] = json_logic::translateNode(jexp["rule"]);

      if (hasComputedVarNames) throw std::runtime_error("unable to work with computed variable names");

      // check that all free variables are prefixed with SELECTED
      for (const boost::json::string& varname : vars)
      {
        if (varname.rfind(SELECTOR, 0) != 0) throw std::logic_error("unknown selector");
        if (varname.find('.') != SELECTOR.size()) throw std::logic_error("unknown selector.");
      }

      queries.emplace_back(std::move(ast));
    }

    std::vector<int>     selectedRows;
    std::int64_t         rownum = 0;

    for (const auto& row : dataset)
    {
      if (!row.is_object()) throw std::logic_error("Not a json::object");

      const auto& rowobj = row.as_object();
      const int   selLen = (SELECTOR.size() + 1);
      auto varLookup = [&rowobj,selLen,rownum](const boost::json::string& colname, int) -> json_logic::ValueExpr
                       {
                         // \todo match selector instead of skipping it
                         std::string_view col{colname.begin() + selLen, colname.size() - selLen};
                         auto             pos = rowobj.find(col);

                         if (pos == rowobj.end())
                         {
                           CXX_UNLIKELY;
                           return (col == "rowid") ? json_logic::toValueExpr(rownum)
                                                   : json_logic::toValueExpr(nullptr);
                         }

                         return toValueExpr(pos->value());
                       };

      auto rowPredicate = [varLookup](json_logic::AnyExpr& query) -> bool
                          {
                            json_logic::ValueExpr exp = json_logic::calculate(query, varLookup);

                            return !json_logic::unpackValue<bool>(std::move(exp));
                          };

      const std::vector<json_logic::AnyExpr>::iterator lim = queries.end();
      const std::vector<json_logic::AnyExpr>::iterator pos = std::find_if(queries.begin(), lim, rowPredicate);

      if (pos == lim)
      {
        res.push_back(rownum);

        if (int(res.size()) == numrows) { CXX_UNLIKELY; break; }
      }

      ++rownum;
    }

    return res;
  }

  std::vector<int>
  generateIndexN(std::vector<int> v, int count)
  {
    v.reserve(count);
    std::generate_n( std::back_inserter(v), count,
                     [i=-1]() mutable -> int { return ++i; }
                   );

    return v;
  }

  inline
  std::vector<int>
  getSelectedRows(const clippy::clippy& clip, const vector_json_type& vec, int numrows = std::numeric_limits<int>::max())
  {
    if (!clip.has_state(ST_SELECTED))
    {
      CXX_UNLIKELY;
      return generateIndexN(std::vector<int>{}, std::min(numrows, int(vec.size())));
    }

    JsonExpression jsonExpression = clip.get_state<JsonExpression>(ST_SELECTED);

    return computeSelected(vec, jsonExpression, numrows);
  }
}

int ygm_main(ygm::comm& world, int argc, char** argv);

int main(int argc, char** argv)
{
  ygm::comm world(&argc, &argv);

  return ygm_main(world, argc, argv);
}


