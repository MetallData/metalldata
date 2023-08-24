// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>

#include <boost/json.hpp>
// #include <metall/json/parse.hpp>

#include "mjl-common.hpp"
#include "clippy/clippy-eval.hpp"

namespace xpr = experimental;

namespace {
const std::string methodName     = "set";
const std::string ARG_COLUMN     = "column";
const std::string ARG_EXPRESSION = "expression";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{
      methodName, "For all selected rows, set a field to a (computed) value."};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  clip.add_required<std::string>(ARG_COLUMN, "output column");
  clip.add_required<boost::json::object>(ARG_EXPRESSION,
                                         "output value expression");

  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    metall_manager mm{metall::open_only, dataLocation.data(), MPI_COMM_WORLD};
    xpr::metall_json_lines lines{mm, world};
    auto                   alloc = lines.get_allocator();
    const std::size_t      updated =
        lines.filter(filter(world.rank(), clip, KEYS_SELECTOR))
            .set(updater(world.rank(), clip, ARG_COLUMN, ARG_EXPRESSION,
                         KEYS_SELECTOR, alloc));

    if (world.rank() == 0) {
      clip.to_return(updated);
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
