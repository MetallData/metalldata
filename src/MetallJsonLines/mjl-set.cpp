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

namespace bj  = boost::json;
namespace xpr = experimental;

namespace {
const std::string METHOD_DESC    = "set";
const std::string METHOD_NAME    = "For all selected rows, set a field to a (computed) value.";

const parameter_description<std::string> arg_column{"column", "output column"};
const parameter_description<bj::object>  arg_expression{"expression", "output value expression"};
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  arg_column.register_with_clippy(clip);
  arg_expression.register_with_clippy(clip);

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
            .set(updater(world.rank(), arg_column.get(clip), arg_expression.get(clip),
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
