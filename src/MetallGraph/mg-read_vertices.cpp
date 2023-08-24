// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mg-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "read_vertices";
const std::string METHOD_DESC =
    "Imports Json Data from files into the vertex container.";

using ARG_VERTEX_FILES_TYPE             = std::vector<std::string>;
const std::string ARG_VERTEX_FILES_NAME = "files";
const std::string ARG_VERTEX_FILES_DESC =
    "A list of Json files that will be imported as vertices.";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");

  clip.add_required<ARG_VERTEX_FILES_TYPE>(ARG_VERTEX_FILES_NAME,
                                           ARG_VERTEX_FILES_DESC);
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const ARG_VERTEX_FILES_TYPE vertexFiles =
        clip.get<ARG_VERTEX_FILES_TYPE>(ARG_VERTEX_FILES_NAME);
    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    metall_manager mm{metall::open_only, dataLocation.data(), MPI_COMM_WORLD};
    xpr::metall_graph         g{mm, world};
    const xpr::import_summary summary = g.read_vertex_files(vertexFiles);

    if (world.rank() == 0) {
      clip.to_return(summary.asJson());
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
