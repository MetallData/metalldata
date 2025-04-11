// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief This file contains common code for the MetallJsonLines implementation

#pragma once

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY 1
#endif

#include <limits>
#include <optional>
#include <string>
#include <system_error>

#include <ygm/comm.hpp>

#include <experimental/cxx-compat.hpp>

#define WITH_YGM 1

#include <clippy/clippy.hpp>
#include <jsonlogic/src.hpp>

#include "MetallJsonLines.hpp"

using JsonExpression = std::vector<boost::json::object>;

// \todo should we use JsonExpression also to describe the columns?
using ColumnSelector = std::vector<std::string>;

namespace {
const std::string MJL_CLASS_NAME     = "MetallJsonLines";
const std::string ST_METALL_LOCATION = "metall_location";
const std::string ST_SELECTED        = "selected";
const std::string KEYS_SELECTOR      = "keys";

CXX_MAYBE_UNUSED
jsonlogic::any_expr to_value_expr(
    const experimental::metall_json_lines::accessor_type& el) {
  if (el.is_int64()) return jsonlogic::to_expr(el.as_int64());
  if (el.is_uint64()) return jsonlogic::to_expr(el.as_uint64());
  if (el.is_double()) return jsonlogic::to_expr(el.as_double());
  if (el.is_null()) return jsonlogic::to_expr(nullptr);

  assert(el.is_string());  // \todo array

  const auto& str = el.as_string();

  return jsonlogic::to_expr(boost::json::string(str.begin(), str.end()));
}

template <class MetallJsonObjectT>
CXX_MAYBE_UNUSED jsonlogic::any_expr eval_path(std::string_view         path,
                                                 const MetallJsonObjectT& obj) {
  if (auto pos = obj.find(path); pos != obj.end())
    return to_value_expr(pos->value());

  std::size_t selpos = path.find('.');

  if (selpos == std::string::npos) return jsonlogic::to_expr(nullptr);

  std::string_view selector = path.substr(0, selpos);
  std::string_view suffix   = path.substr(selpos + 1);

  return eval_path(suffix, obj.at(selector).as_object());
}

CXX_MAYBE_UNUSED
auto variable_lookup(
    experimental::metall_json_lines::accessor_type::object_accessor objacc,
    std::string_view selectPrefix, std::size_t rownum, std::size_t rank) {
  return [objacc, rownum, rank, selLen = (selectPrefix.size() + 1)](
             const boost::json::value& colv, int) -> jsonlogic::any_expr {
    // \todo match selector instead of skipping it
    const auto&      colname = colv.as_string();
    std::string_view col{colname.begin() + selLen, colname.size() - selLen};

    if (auto pos = objacc.find(col); pos != objacc.end()) {
      CXX_LIKELY;
      return to_value_expr(pos->value());
    }

    if (col == "rowid") return jsonlogic::to_expr(rownum);
    if (col == "mpiid") return jsonlogic::to_expr(std::int64_t(rank));

    return eval_path(col, objacc);
  };
}

inline auto variable_lookup(
    experimental::metall_json_lines::accessor_type rowval,
    std::string_view selectPrefix, std::size_t rownum, std::size_t rank)
    -> decltype(variable_lookup(rowval.as_object(), selectPrefix, rownum,
                                rank)) {
  if (!rowval.is_object())
    throw std::logic_error("Entry is not a json::object");

  return variable_lookup(rowval.as_object(), selectPrefix, rownum, rank);
}

CXX_MAYBE_UNUSED
std::vector<experimental::metall_json_lines::filter_type> filter(
    std::size_t rank, JsonExpression jsonExpr,
    std::string_view selectPrefix = KEYS_SELECTOR) {
  using ResultType       = decltype(filter(rank, jsonExpr, selectPrefix));
  using BJVectorIterator = std::vector<boost::json::string>::iterator;

  ResultType               res;
  boost::json::string_view boostSelectPrefix(&*selectPrefix.begin(),
                                             selectPrefix.size());

  // prepare AST
  for (boost::json::object& jexp : jsonExpr) {
    auto [ast, vars, hasComputedVarNames] =
        jsonlogic::create_logic(jexp["rule"]);

    if (hasComputedVarNames)
      throw std::runtime_error("unable to work with computed variable names");

    // check that all free variables are prefixed with SELECTED
    auto varcheck =
        [boostSelectPrefix](const boost::json::string& varname) -> bool {
      return (varname.rfind(boostSelectPrefix, 0) == 0 &&
              varname.find('.') == boostSelectPrefix.size());
    };
    BJVectorIterator varlim = vars.end();
    const bool       useRule =
        varlim == std::find_if_not(vars.begin(), varlim, varcheck);

    if (!useRule) continue;

    // the repackaging requirement seems to be a deficiency in the C++
    //   standard, which does not allow lambda environments with unique_ptr
    //   be converted into a std::function - which requires copyability.
    jsonlogic::expr*                 rawexpr = ast.release();
    std::shared_ptr<jsonlogic::expr> pred{rawexpr};

    res.emplace_back([rank, selectPrefix, pred = std::move(pred)](
                         std::size_t rownum,
                         const experimental::metall_json_lines::accessor_type&
                             rowval) mutable -> bool {
      auto varLookup = variable_lookup(rowval, selectPrefix, rownum, rank);

      return jsonlogic::unpack_value<bool>(
          jsonlogic::apply(*pred, varLookup));
    });
  }

  return res;
}

inline std::vector<experimental::metall_json_lines::filter_type> filter(
    std::size_t rank, const clippy::clippy& clip,
    std::string_view selectPrefix = KEYS_SELECTOR) {
  if (!clip.has_state(ST_SELECTED)) {
    CXX_UNLIKELY;
    return {};
  }

  return filter(rank, clip.get_state<JsonExpression>(ST_SELECTED),
                selectPrefix);
}

CXX_MAYBE_UNUSED
experimental::metall_json_lines::metall_projector_type projector(
    ColumnSelector projlist) {
  namespace xpr = experimental;

  // w/o selection list, just return the full object
  if (projlist.empty())
    return [](const xpr::metall_json_lines::accessor_type& el)
               -> boost::json::value {
      return json_bento::value_to<boost::json::value>(el);
    };

  return [fields = std::move(projlist)](
             const xpr::metall_json_lines::accessor_type& el)
             -> boost::json::value {
    assert(el.is_object());
    const auto& frobj = el.as_object();

    boost::json::object res;

    for (const std::string& col : fields) {
      if (const auto fld = frobj.if_contains(col))
        res.emplace(col, json_bento::value_to<boost::json::value>(*fld));
      //~ if (const auto fld = ifContains(frobj, col))
    }

    return res;
  };
}

inline experimental::metall_json_lines::metall_projector_type projector(
    const std::string& projectorKey, clippy::clippy& clip) {
  return projector(clip.get<ColumnSelector>(projectorKey));
}

template <class AllocT>
CXX_MAYBE_UNUSED experimental::metall_json_lines::updater_type updater(
    std::size_t rank, clippy::clippy& clip, const std::string& colkey,
    const std::string& exprkey, std::string_view selectPrefix, AllocT alloc) {
  namespace xpr = experimental;

  std::string         columnName = clip.get<std::string>(colkey);
  boost::json::object columnExpr = clip.get<boost::json::object>(exprkey);
  auto [ast, vars, hasComputedVarNames] =
      jsonlogic::create_logic(columnExpr["rule"]);

  if (hasComputedVarNames)
    throw std::runtime_error("unable to work with computed variable names");

  // the repackaging requirement seems to be a deficiency in the C++
  //   standard, which does not allow lambda environments with unique_ptr
  //   be converted into a std::function - which requires copyability.
  jsonlogic::expr*                 rawexpr = ast.release();
  std::shared_ptr<jsonlogic::expr> oper{rawexpr};

  return [rank, selectPrefix, colName = std::move(columnName),
          op = std::move(oper), objalloc{alloc}](
             std::size_t                           rownum,
             xpr::metall_json_lines::accessor_type rowval) -> void {
    auto varLookup = variable_lookup(rowval, selectPrefix, rownum, rank);
    jsonlogic::any_expr exp    = jsonlogic::apply(*op, varLookup);
    auto                  rowobj = rowval.as_object();
    std::stringstream     jstr;

    jstr << exp;

    rowobj[colName].parse(jstr.str());
  };
}

CXX_MAYBE_UNUSED
inline void append(std::vector<boost::json::object>& lhs,
                   std::vector<boost::json::object>  rhs) {
  if (lhs.size() == 0) return lhs.swap(rhs);

  std::move(rhs.begin(), rhs.end(), std::back_inserter(lhs));
}

/// removes the entire directory \ref loc and its content and synchronizes
/// processes on \ref world after the directory has been removed.
CXX_MAYBE_UNUSED
inline void remove_directory_and_content(ygm::comm&       world,
                                         std::string_view loc) {
  try {
    std::error_code ec;

    if (std::filesystem::is_directory(loc, ec)) {
      // checking ec for 0 is not robust in general (though it may be for this
      // specific use)
      // -> ignore the error and delete everything
      std::filesystem::remove_all(loc, ec);  // may throw std::bad_alloc
    }
  } catch (...) {
  }

  // barrier is needed to make sure that processes accessing the file system
  //   do not allocate before some other process deletes the directory...
  world.barrier();
}

}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv);

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  return ygm_main(world, argc, argv);
}
