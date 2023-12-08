// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements head function to return N entries from a MetallJsoneLines

#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>
//~ #include <ranges>

#include <boost/json.hpp>

#include "mjl-common.hpp"
#include "clippy/clippy-eval.hpp"

namespace xpr = experimental;

namespace {

const std::string METHOD_NAME = "head";
const std::string METHOD_DESC = "Returns n arbitrary rows for which the predicate evaluates to true.";

const parameter_description<int>            arg_num{"num", "Max number of rows returned", 5};
const parameter_description<ColumnSelector> arg_columns{"columns", "projection list (list of columns to put out)", {}};

}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  arg_num.register_with_clippy(clip);
  arg_columns.register_with_clippy(clip);

  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    const std::size_t      numrows = arg_num.get(clip);
    metall_manager         mm{metall::open_read_only, dataLocation.data(),
                      MPI_COMM_WORLD};
    xpr::metall_json_lines lines{mm, world};
    boost::json::value     res =
        lines.filter(filter(world.rank(), clip, KEYS_SELECTOR))
            .head(numrows, projector(arg_columns.get(clip)));

    if (world.rank() == 0) clip.to_return(std::move(res));
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
