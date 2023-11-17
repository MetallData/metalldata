// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mf-common.hpp"
#include "metall_frame.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "read_csv";
const std::string METHOD_DESC =
    "Imports CSV Data from files into the MetallFrame object.";

const std::string ARG_CSV_FILES_NAME = "csv_files";
const std::string ARG_CSV_FILES_DESC =
    "A list of CSV files that will be imported.";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MF_CLASS_NAME, "A " + MF_CLASS_NAME + " class");
  clip.add_required<std::vector<std::string> >(ARG_CSV_FILES_NAME,
                                               ARG_CSV_FILES_DESC);
  clip.add_required_state<std::string>(ST_METALL_LOCATION_NAME,
                                       ST_METALL_LOCATION_DESC);
  clip.add_required_state<std::string>(ST_METALLFRAME_NAME,
                                       ST_METALLFRAME_DESC);

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_frame::metall_manager_type;

    const std::vector<std::string> files = clip.get<std::vector<std::string> >(ARG_CSV_FILES_NAME);
    std::string               dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION_NAME);
    std::string               key = clip.get_state<std::string>(ST_METALLFRAME_NAME);
    metall_manager            mm{metall::open_only, dataLocation.data(), MPI_COMM_WORLD};
    xpr::metall_frame         frame{mm, world, key};
    const xpr::import_summary imp = frame.read_csv_files(files);

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
