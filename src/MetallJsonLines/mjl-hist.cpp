// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Demonstrates how to run Json expr::metall_json_linesession predicates
/// on a MetallJsonLines

#include <boost/json.hpp>
#include "mjl-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME      = "hist";
const std::string METHOD_DOCSTRING = "Make a histogram";

const std::string COL = "col";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DOCSTRING};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  clip.add_required_state<bool>(COL, "Column name");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    const std::string col = clip.get_state<std::string>(COL);

    metall_manager         mm{metall::open_read_only, dataLocation.data(),
                      MPI_COMM_WORLD};
    xpr::metall_json_lines lines{mm, world};
    auto                   histogram = lines.hist(col);

    if (world.rank() == 0) {
      clip.to_return(histogram);
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  } catch (...) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return("unhandled, unknown exception");
  }

  return error_code;
}
