// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Demonstrates how to run Json expr::metall_json_linesession predicates
/// on a MetallJsonLines


#include <boost/json.hpp>
#include "mf-common.hpp"


namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "count";
const std::string METHOD_DESC =
    "Counts the number of rows where the current selection criteria is true.";

const std::string ARG_COUNT_ALL_NAME = "count_all";
const std::string ARG_COUNT_ALL_DESC = "if true, the selection criteria is ignored";
const bool        ARG_COUNT_ALL_DFLT = false;
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MF_CLASS_NAME, "A " + MF_CLASS_NAME + " class");
  clip.add_optional<std::vector<std::string> >(ARG_COUNT_ALL_NAME,
                                               ARG_COUNT_ALL_DESC,
                                               ARG_COUNT_ALL_DFLT);
  clip.add_required_state<std::string>(ST_METALL_LOCATION_NAME,
                                       ST_METALL_LOCATION_DESC);
  clip.add_required_state<std::string>(ST_METALLFRAME_NAME,
                                       ST_METALLFRAME_DESC);

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_frame::metall_manager_type;
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const bool        countAll = clip.get<bool>(COUNT_ALL_NAME);
    std::string       dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION_NAME);
    std::string       key = clip.get_state<std::string>(ST_METALLFRAME_NAME);
    metall_manager    mm{metall::open_only, dataLocation.data(), MPI_COMM_WORLD};
    xpr::metall_frame frame{mm, world, key};
    const std::size_t res =
                         countAll ? frame.count()
                                  : frame.filter(filter(world.rank(), clip)).count();

    if (world.rank() == 0) {
      clip.to_return(res);
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


