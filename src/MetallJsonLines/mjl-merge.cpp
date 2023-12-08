// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements joining two MetallJsonLines data sets.

#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
#include <chrono>
#include <tuple>
#include <vector>

//~ #include <bit>
//
#include "mjl-common.hpp"
#include "clippy/clippy.hpp"

#include "MetallJsonLines-merge.hpp"

namespace xpr = experimental;

namespace {
using StringVector = std::vector<std::string>;

const bool LOG_TIMING = false;

const std::string METHOD_NAME = "merge";
const std::string METHOD_DESC = "For all selected rows, set a field to a (computed) value.";

const parameter_description<bj::object> arg_output{"output", "result MetallJsonLines object; any existing data will be overwritten"};
const parameter_description<bj::object> arg_left{"left", "left hand side MetallJsonLines object"};
const parameter_description<bj::object> arg_right{"right", "right hand side MetallJsonLines object"};
const parameter_description<ColumnSelector> arg_on{ "on",
                                                    "list of column names on which to join on "
                                                    "(overruled by left_on/right_on)",
                                                    {}
                                                  };
const parameter_description<ColumnSelector> arg_left_on{ "left_on",
                                                         "list of columns on which to join left MetallJsonLines",
                                                         {}
                                                       };
const parameter_description<ColumnSelector> arg_right_on{ "right_on",
                                                          "list of columns on which to join right MetallJsonLines",
                                                          {}
                                                        };
const parameter_description<ColumnSelector> arg_left_columns{ "left_columns",
                                                              "projection list of the left input frame",
                                                              {}
                                                            };
const parameter_description<ColumnSelector> arg_right_columns{ "right_columns",
                                                               "projection list of the left input frame",
                                                               {}
                                                             };


//~ const std::string    ARG_SUFFIXES     = "suffixes";
//~ const StringVector   DEFAULT_SUFFIXES{"_x", "_y"};

template <class _allocator_type>
mtljsn::value<_allocator_type> convertJsonTypeTo(
    const bj::value& orig, const mtljsn::value<_allocator_type>& /*model*/) {
  return mtljsn::value_from(orig, _allocator_type{});
}

JsonExpression selectionCriteria(bj::object& obj) {
  return valueAt<JsonExpression>(obj, "__clippy_type__", "state", ST_SELECTED);
}

using time_point = std::chrono::time_point<std::chrono::system_clock>;

struct Timer : std::vector<std::tuple<const char*, time_point> >
{
  using base = std::vector<std::tuple<const char*, time_point> >;

  Timer()
  : base()
  {
    segment("");
  }

  void segment(const char* desc)
  {
    if (LOG_TIMING)
      base::emplace_back(desc, std::chrono::system_clock::now());
  }
};

std::ostream& operator<<(std::ostream& os, Timer& timer)
{
  for (int i = 1; i < timer.size(); ++i)
  {
    os << std::get<0>(timer.at(i)) << ' '
       << std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(timer.at(i)) - std::get<1>(timer.at(i-1))).count()
       << "   ";
  }

  os << "  = "
     << std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(timer.at(timer.size()-1)) - std::get<1>(timer.at(0))).count();

  return os;
};

}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  Timer          timer;
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  // model this as free-standing function
  //~ clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  // arguments
  arg_output.register_with_clippy(clip);
  arg_left.register_with_clippy(clip);
  arg_right.register_with_clippy(clip);
  arg_on.register_with_clippy(clip);
  arg_left_on.register_with_clippy(clip);
  arg_right_on.register_with_clippy(clip);
  arg_left_columns.register_with_clippy(clip);
  arg_right_columns.register_with_clippy(clip);


  // currently unsupported optional arguments
  // clip.add_optional(ARG_HOW, "join method:
  // {'left'|'right'|'outer'|'inner'|'cross']} default: inner", DEFAULT_HOW);

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  timer.segment("startup");

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    // argument processing
    bj::object lhsObj       = arg_left.get(clip);
    bj::object rhsObj       = arg_right.get(clip);

    ColumnSelector argsOn   = arg_on.get(clip);
    ColumnSelector argLhsOn = arg_left_on.get(clip);
    ColumnSelector argRhsOn = arg_right_on.get(clip);

    ColumnSelector projLhs  = arg_left_columns.get(clip);
    ColumnSelector projRhs  = arg_right_columns.get(clip);

    // argument error checking
    //   \todo move to validation
    if (argLhsOn.empty() && argsOn.empty())
      throw std::runtime_error{"on-columns unspecified for left frame."};

    if (argRhsOn.empty() && argsOn.empty())
      throw std::runtime_error{"on-columns unspecified for right frame."};

    const ColumnSelector& lhsOn = argLhsOn.empty() ? argsOn : argLhsOn;
    const ColumnSelector& rhsOn = argRhsOn.empty() ? argsOn : argRhsOn;

    if (lhsOn.size() != rhsOn.size())
      throw std::runtime_error{
          "Number of columns of Left_On and Right_on differ"};

    timer.segment("args");

    const bj::string& lhsLoc = valueAt<bj::string>(lhsObj, "__clippy_type__",
                                                   "state", ST_METALL_LOCATION);
    metall_manager    lhsMgr{metall::open_read_only, lhsLoc.data(),
                          MPI_COMM_WORLD};
    xpr::metall_json_lines lhsVec{lhsMgr, world};
    lhsVec.filter(
        filter(world.rank(), selectionCriteria(lhsObj), KEYS_SELECTOR));

    timer.segment("lhs");
    const bj::string& rhsLoc = valueAt<bj::string>(rhsObj, "__clippy_type__",
                                                   "state", ST_METALL_LOCATION);
    timer.segment("rhs-loc");
    metall_manager    rhsMgr{metall::open_read_only, rhsLoc.data(),
                          MPI_COMM_WORLD};
    timer.segment("rhs-mgr");
    xpr::metall_json_lines rhsVec{rhsMgr, world};
    timer.segment("rhs-open");
    rhsVec.filter(
        filter(world.rank(), selectionCriteria(rhsObj), KEYS_SELECTOR));
    timer.segment("rhs-filter");

    bj::object        outObj = arg_output.get(clip);
    const bj::string& outLoc = valueAt<bj::string>(outObj, "__clippy_type__",
                                                   "state", ST_METALL_LOCATION);
    std::string_view  outLocVw(outLoc.data(), outLoc.size());

    timer.segment("out");

    // \todo instead of deleting the entire directory tree, just try
    //       to open the output location and clear the content
    //       then add argument whether existing data should be kept or
    //       overwritten
    //~ remove_directory_and_content(world, outLocVw);

    timer.segment("out-rmdir");

    metall_manager outMgr{metall::open_only, outLoc.data(), MPI_COMM_WORLD};

    timer.segment("out-mgr");

    xpr::metall_json_lines outVec{outMgr, world};

    timer.segment("out-open");

    const std::size_t      totalMerged =
        xpr::merge(outVec, lhsVec, rhsVec, lhsOn, rhsOn, std::move(projLhs),
                   std::move(projRhs));

    timer.segment("merge");

    if (world.rank() == 0) {
      clip.to_return(totalMerged);
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  if (LOG_TIMING)
  {
    std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};
    timer.segment("post");

    logfile << timer << std::endl;
  }

  return error_code;
}
