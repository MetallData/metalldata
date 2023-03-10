// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements joining two MetallJsonLines data sets.

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>
#include <functional>
//~ #include <bit>

#include "mjl-common.hpp"
#include "clippy/clippy.hpp"

#include "MetallJsonLines-merge.hpp"


namespace xpr     = experimental;


namespace
{
  const bool DEBUG_TRACE = false;

  using StringVector = std::vector<std::string>;

  const std::string    methodName       = "merge";
  const std::string    ARG_OUTPUT       = "output";
  const std::string    ARG_LEFT         = "left";
  const std::string    ARG_RIGHT        = "right";

  const std::string    ARG_HOW          = "how";
  const std::string    DEFAULT_HOW      = "inner";

  const std::string    ARG_ON           = "on";
  const std::string    ARG_LEFT_ON      = "left_on";
  const std::string    ARG_RIGHT_ON     = "right_on";

  const std::string    COLUMNS_LEFT     = "left_columns";
  const std::string    COLUMNS_RIGHT    = "right_columns";

  const ColumnSelector DEFAULT_COLUMNS  = {};

  //~ const std::string    ARG_SUFFIXES     = "suffixes";
  //~ const StringVector   DEFAULT_SUFFIXES{"_x", "_y"};
} // anonymous


int ygm_main(ygm::comm& world, int argc, char** argv)
{
  using time_point = std::chrono::time_point<std::chrono::system_clock>;

  int            error_code = 0;
  clippy::clippy clip{methodName, "For all selected rows, set a field to a (computed) value."};

  // model this as free-standing function
  //~ clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  // required arguments
  clip.add_required<bj::object>(ARG_OUTPUT,     "result MetallJsonLines object; any existing data will be overwritten");
  clip.add_required<bj::object>(ARG_LEFT,       "right hand side MetallJsonLines object");
  clip.add_required<bj::object>(ARG_RIGHT,      "left hand side MetallJsonLines object");

  // future optional arguments
  // \todo should these be json expressions
  clip.add_optional<ColumnSelector>(ARG_ON,        "list of column names on which to join on (overruled by left_on/right_on)", DEFAULT_COLUMNS);
  clip.add_optional<ColumnSelector>(ARG_LEFT_ON,   "list of columns on which to join left MetallJsonLines", DEFAULT_COLUMNS);
  clip.add_optional<ColumnSelector>(ARG_RIGHT_ON,  "list of columns on which to join right MetallJsonLines", DEFAULT_COLUMNS);

  // columns to join on
  clip.add_optional<ColumnSelector>(COLUMNS_LEFT,  "projection list of the left input frame", DEFAULT_COLUMNS);
  clip.add_optional<ColumnSelector>(COLUMNS_RIGHT, "projection list of the right input frame", DEFAULT_COLUMNS);

  // currently unsupported optional arguments
  // clip.add_optional(ARG_HOW, "join method: {'left'|'right'|'outer'|'inner'|'cross']} default: inner", DEFAULT_HOW);

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    // argument processing
    bj::object     lhsObj = clip.get<bj::object>(ARG_LEFT);
    bj::object     rhsObj = clip.get<bj::object>(ARG_RIGHT);

    ColumnSelector argsOn   = clip.get<ColumnSelector>(ARG_ON);
    ColumnSelector argLhsOn = clip.get<ColumnSelector>(ARG_LEFT_ON);
    ColumnSelector argRhsOn = clip.get<ColumnSelector>(ARG_RIGHT_ON);

    ColumnSelector projLhs = clip.get<ColumnSelector>(COLUMNS_LEFT);
    ColumnSelector projRhs = clip.get<ColumnSelector>(COLUMNS_RIGHT);

    // argument error checking
    //   \todo move to validation
    if (argLhsOn.empty() && argsOn.empty())
      throw std::runtime_error{"on-columns unspecified for left frame."};

    if (argRhsOn.empty() && argsOn.empty())
      throw std::runtime_error{"on-columns unspecified for right frame."};

    const ColumnSelector& lhsOn = argLhsOn.empty() ? argsOn : argLhsOn;
    const ColumnSelector& rhsOn = argRhsOn.empty() ? argsOn : argRhsOn;

    if (lhsOn.size() != rhsOn.size())
      throw std::runtime_error{"Number of columns of Left_On and Right_on differ"};

    const bj::string&     lhsLoc = valueAt<bj::string>(lhsObj, "__clippy_type__", "state", ST_METALL_LOCATION);
    xpr::MetallJsonLines  lhsVec{MPI_COMM_WORLD, world, metall::open_read_only, lhsLoc.c_str()};
    lhsVec.filter(filter(world.rank(), selectionCriteria(lhsObj), SELECTOR));

    const bj::string&     rhsLoc = valueAt<bj::string>(rhsObj, "__clippy_type__", "state", ST_METALL_LOCATION);
    xpr::MetallJsonLines  rhsVec{MPI_COMM_WORLD, world, metall::open_read_only, rhsLoc.c_str()};
    rhsVec.filter(filter(world.rank(), selectionCriteria(rhsObj), SELECTOR));

    bj::object            outObj = clip.get<bj::object>(ARG_OUTPUT);
    const bj::string&     outLoc = valueAt<bj::string>(outObj, "__clippy_type__", "state", ST_METALL_LOCATION);

    // \todo overwrite needed?
    xpr::MetallJsonLines::createOverwrite(MPI_COMM_WORLD, world, std::string_view{outLoc.begin(), outLoc.size()});
    xpr::MetallJsonLines  outVec{MPI_COMM_WORLD, world, metall::open_only, outLoc.c_str()};
    const std::size_t     totalMerged = xpr::merge( outVec, lhsVec, rhsVec,
                                                    lhsOn, rhsOn,
                                                    std::move(projLhs), std::move(projRhs)
                                                  );

    if (world.rank() == 0)
    {
      clip.to_return(totalMerged);
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


