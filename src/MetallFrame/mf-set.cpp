// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>

#include <boost/json.hpp>
#include <metall/container/experimental/json/parse.hpp>

#include "mf-common.hpp"
#include "clippy/clippy-eval.hpp"


namespace bjsn    = boost::json;
namespace mtlutil = metall::utility;
namespace mtljsn  = metall::container::experimental::json;
namespace jl      = json_logic;

namespace
{
const std::string methodName     = "set";
const std::string ARG_COLUMN     = "column";
const std::string ARG_EXPRESSION = "expression";
} // anonymous



int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "For all selected rows, set a field to a (computed) value."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_required<std::string>(ARG_COLUMN, "output column");
  clip.add_required<bjsn::object>(ARG_EXPRESSION, "output value expression");

  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    std::string                 columnName = clip.get<std::string>(ARG_COLUMN);
    bjsn::object                columnExpr = clip.get<bjsn::object>(ARG_EXPRESSION);
    mtlutil::metall_mpi_adaptor manager(metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD);
    vector_json_type&           vec = jsonVector(manager);
    auto&                       mgr = manager.get_local_manager();
    auto [ast, vars, hasComputedVarNames] = jl::translateNode(columnExpr["rule"]);

    if (hasComputedVarNames) throw std::runtime_error("unable to work with computed variable names");

    int                         updCount = 0;

    auto updateFn = [&updCount, &ast, &columnName, objalloc{mgr.get_allocator()}]
                    (int rownum, const vector_json_type::value_type& rowval) -> void
                    {
                      ++updCount;

                      auto& rowobj     = const_cast<vector_json_type::value_type&>(rowval).as_object();
                      const int selLen = (SELECTOR.size() + 1);
                      auto  varLookup  = [&rowobj,selLen,rownum]
                                         (const bjsn::string& colname, int) -> jl::ValueExpr
                                         {
                                           // \todo match selector instead of skipping it
                                           std::string_view col{colname.begin() + selLen, colname.size() - selLen};
                                           auto             pos = rowobj.find(col);

                                           if (pos == rowobj.end())
                                           {
                                             CXX_UNLIKELY;
                                             return (col == "rowid") ? jl::toValueExpr(std::int64_t{rownum})
                                                                     : jl::toValueExpr(nullptr);
                                           }

                                           return toValueExpr(pos->value());
                                         };

                      std::stringstream jstr;
                      jl::ValueExpr     exp = jl::calculate(ast, varLookup);

                      jstr << exp;

                      // return metall::container::experimental::json::value_from(std::move(bj_value), allocator);
                      rowobj[columnName] = mtljsn::parse(jstr.str(), objalloc);
                    };


    if (!clip.has_state(ST_SELECTED))
      std::for_each( vec.begin(), vec.end(),
                     [updateFn{std::move(updateFn)}, rownum{0}]
                     (const vector_json_type::value_type& row) mutable->void
                     {
                       updateFn(++rownum, row);
                     }
                   );
    else
      forAllSelected( updateFn, world.rank(), vec, clip.get_state<JsonExpression>(ST_SELECTED) );

    world.barrier(); // necessary?

    const int totalUpdated = world.all_reduce_sum(updCount);

    if (world.rank() == 0)
    {
      std::stringstream msg;

      msg << "updated column " << columnName << " in " << totalUpdated << " entries"
          << std::endl;
      clip.to_return(msg.str());
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


