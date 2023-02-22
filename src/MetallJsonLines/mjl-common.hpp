// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief This file contains common code for the MetallJsonLines implementation

#pragma once

#include <string>
#include <limits>

#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <metall/container/vector.hpp>
#include <metall/container/experimental/json/json.hpp>

#include <ygm/comm.hpp>

#include <experimental/cxx-compat.hpp>

#define WITH_YGM 1

#include <clippy/clippy.hpp>
#include <clippy/clippy-eval.hpp>

#include "MetallJsonLines.hpp"


using JsonExpression   = std::vector<boost::json::object>;

// \todo should we use JsonExpression also to describe the columns?
using ColumnSelector = std::vector<std::string>;

namespace
{
const std::string CLASS_NAME         = "MetallJsonLines";
const std::string ST_METALL_LOCATION = "metall_location";
const std::string ST_SELECTED        = "selected";
const std::string SELECTOR           = "keys";

template <class JsonObject>
inline
auto
ifContains(const JsonObject& obj, const std::string& name) -> decltype(&obj.at(name))
{
  auto pos = obj.find(name);

  return (pos == obj.end()) ? nullptr : &pos->value();
}

CXX_MAYBE_UNUSED
json_logic::ValueExpr
toValueExpr(const experimental::MetallJsonLines::value_type& el)
{
  if (el.is_int64())  return json_logic::toValueExpr(el.as_int64());
  if (el.is_uint64()) return json_logic::toValueExpr(el.as_uint64());
  if (el.is_double()) return json_logic::toValueExpr(el.as_double());
  if (el.is_null())   return json_logic::toValueExpr(nullptr);

  assert(el.is_string()); // \todo array

  const auto& str = el.as_string();

  return json_logic::toValueExpr(boost::json::string(str.begin(), str.end()));
}



template <class MetallJsonObjectT>
CXX_MAYBE_UNUSED
auto variableLookup(const MetallJsonObjectT& rowobj, std::size_t rownum, std::size_t rank)
{
  return [&rowobj,rownum,rank,selLen=(SELECTOR.size() + 1)]
         (const boost::json::string& colname, int) -> json_logic::ValueExpr
         {
           // \todo match selector instead of skipping it
           std::string_view col{colname.begin() + selLen, colname.size() - selLen};
           auto             pos = rowobj.find(col);

           if (pos == rowobj.end())
           {
             CXX_UNLIKELY;
             if (col == "rowid") return json_logic::toValueExpr(rownum);
             if (col == "mpiid") return json_logic::toValueExpr(std::int64_t(rank));

             return json_logic::toValueExpr(nullptr);
           }

           return toValueExpr(pos->value());
         };
}

inline
auto variableLookup(const experimental::MetallJsonLines::value_type& rowval, std::size_t rownum, std::size_t rank)
     -> decltype(variableLookup(rowval.as_object(), rownum, rank))
{
  if (!rowval.is_object()) throw std::logic_error("Entry is not a json::object");

  return variableLookup(rowval.as_object(), rownum, rank);
}

CXX_MAYBE_UNUSED
std::vector<experimental::MetallJsonLines::filter_type>
filter(std::size_t rank, JsonExpression jsonExpr)
{
  using ResultType = decltype(filter(rank, jsonExpr));

  ResultType res;

  // prepare AST
  for (boost::json::object& jexp : jsonExpr)
  {
    auto [ast, vars, hasComputedVarNames] = json_logic::translateNode(jexp["rule"]);

    if (hasComputedVarNames) throw std::runtime_error("unable to work with computed variable names");

    // check that all free variables are prefixed with SELECTED
    for (const boost::json::string& varname : vars)
    {
      if (varname.rfind(SELECTOR, 0) != 0) throw std::logic_error("unknown selector");
      if (varname.find('.') != SELECTOR.size()) throw std::logic_error("unknown selector.");
    }

    // the repackaging requirement seems to be a deficiency in the C++
    //   standard, which does not allow lambda environments with unique_ptr
    //   be converted into a std::function - which requires copyability.
    json_logic::Expr*                 rawexpr = ast.release();
    std::shared_ptr<json_logic::Expr> pred{rawexpr};

    res.emplace_back( [rank, pred = std::move(pred)]
                      (std::size_t rownum, const experimental::MetallJsonLines::value_type& rowval) mutable -> bool
                      {
                        auto varLookup = variableLookup(rowval, rownum, rank);

                        return json_logic::unpackValue<bool>(json_logic::calculate(*pred, varLookup));
                      }
                    );
  }

  return res;
}

inline
std::vector<experimental::MetallJsonLines::filter_type>
filter(std::size_t rank, const clippy::clippy& clip)
{
  namespace xpr = experimental;

  if (!clip.has_state(ST_SELECTED))
  {
    CXX_UNLIKELY;
    return {};
  }

  return filter(rank, clip.get_state<JsonExpression>(ST_SELECTED));
}

CXX_MAYBE_UNUSED
experimental::MetallJsonLines::metall_projector_type
projector(ColumnSelector projlist)
{
  namespace xpr = experimental;

  // w/o selection list, just return the full object
  if (projlist.empty())
    return [](const xpr::MetallJsonLines::value_type& el) -> boost::json::value
           {
             return metall::container::experimental::json::value_to<boost::json::value>(el);
           };

  return [fields = std::move(projlist)]
         (const xpr::MetallJsonLines::value_type& el) -> boost::json::value
         {
           assert (el.is_object());
           const auto& frobj = el.as_object();

           boost::json::object res;

           for (const std::string& col : fields)
           {
             if (const auto* fld = ifContains(frobj, col))
               res.emplace(col, metall::container::experimental::json::value_to<boost::json::value>(*fld));
           }

           return res;
         };
}

inline
experimental::MetallJsonLines::metall_projector_type
projector(const std::string& selectorName, clippy::clippy& clip)
{
  return projector(clip.get<ColumnSelector>(selectorName));
}

template <class AllocT>
CXX_MAYBE_UNUSED
experimental::MetallJsonLines::updater_type
updater( std::size_t rank,
         clippy::clippy& clip,
         const std::string& colkey,
         const std::string& exprkey,
         AllocT alloc
       )
{
  namespace xpr = experimental;

  std::string         columnName = clip.get<std::string>(colkey);
  boost::json::object columnExpr = clip.get<boost::json::object>(exprkey);
  auto [ast, vars, hasComputedVarNames] = json_logic::translateNode(columnExpr["rule"]);

  if (hasComputedVarNames) throw std::runtime_error("unable to work with computed variable names");

  // the repackaging requirement seems to be a deficiency in the C++
  //   standard, which does not allow lambda environments with unique_ptr
  //   be converted into a std::function - which requires copyability.
  json_logic::Expr*                 rawexpr = ast.release();
  std::shared_ptr<json_logic::Expr> oper{rawexpr};

  return [rank, colName=std::move(columnName), op=std::move(oper), objalloc{alloc}]
         (int rownum, xpr::MetallJsonLines::value_type& rowval) -> void
         {
           auto                  varLookup = variableLookup(rowval, rownum, rank);
           json_logic::ValueExpr exp       = json_logic::calculate(*op, varLookup);
           auto&                 rowobj    = rowval.as_object();
           std::stringstream     jstr;

           jstr << exp;

           // return metall::container::experimental::json::value_from(*exp, objalloc);
           rowobj[colName] = metall::container::experimental::json::parse(jstr.str(), objalloc);
         };
}

}

int ygm_main(ygm::comm& world, int argc, char** argv);

int main(int argc, char** argv)
{
  ygm::comm world(&argc, &argv);

  return ygm_main(world, argc, argv);
}

