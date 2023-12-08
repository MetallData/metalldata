// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mjl-common.hpp"

namespace xpr = experimental;

namespace {
using StringVector = std::vector<std::string>;

const std::string METHOD_NAME = "read_json";
const std::string METHOD_DESC =
    "Imports Json Data from files into the MetallJsonLines object.";

const parameter_description<StringVector> arg_json_files{ "json_files",
                                                          "A list of Json files that will be imported."
                                                        };
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  arg_json_files.register_with_clippy(clip);

  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::vector<std::string> files = arg_json_files.get(clip);
    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    metall_manager mm{metall::open_only, dataLocation.data(), MPI_COMM_WORLD};
    xpr::metall_json_lines    lines{mm, world};
    const xpr::import_summary imp = lines.read_json_files(files);

    if (world.rank() == 0) {
      assert(imp.rejected() == 0);
      clip.to_return(imp.imported());
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
