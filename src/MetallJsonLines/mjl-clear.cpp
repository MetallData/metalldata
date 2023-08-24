// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mjl-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "clear";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{
      METHOD_NAME,
      "Erases ALL elements in MetallJsonLines (selection is ignored)."};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

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

    lines /* \todo .filter(filter(world.rank(), clip, KEYS_SELECTOR)) */
        .clear();

    assert(lines.count() == 0);

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
