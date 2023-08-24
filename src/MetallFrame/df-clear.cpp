// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "df-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "clear";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, "Erases all elements in the MetallFrame."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");
  clip.add_required_state<std::string>(ST_METALLFRAME_NAME, "Metallframe2 key");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    std::string location = clip.get_state<std::string>(ST_METALL_LOCATION);
    std::string key      = clip.get_state<std::string>(ST_METALLFRAME_NAME);
    std::unique_ptr<xpr::DataFrame> dfp =
        makeDataFrame(false /* existing */, location, key);

    dfp->clear();

    assert(dfp->rows() == 0);

    if (world.rank() == 0) {
      std::stringstream msg;

      msg << "all rows deleted." << std::flush;
      clip.to_return(msg.str());
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
