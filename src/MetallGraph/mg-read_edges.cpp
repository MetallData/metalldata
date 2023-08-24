// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mg-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "read_edges";
const std::string METHOD_DESC =
    "Imports Json Data from files into the vertex container.";

using ARG_EDGE_FILES_TYPE             = std::vector<std::string>;
const std::string ARG_EDGE_FILES_NAME = "files";
const std::string ARG_EDGE_FILES_DESC =
    "A list of Json files that will be imported as edges.";

using ARG_AUTO_VERTEX_TYPE             = std::vector<std::string>;
const std::string ARG_AUTO_VERTEX_NAME = "autoVertices";
const std::string ARG_AUTO_VERTEX_DESC =
    "two field names from which the vertices are generated";
const ARG_AUTO_VERTEX_TYPE ARG_AUTO_VERTEX_DFLT = {};

//~ using                              ARG_AUTO_SRC_VERTEX_TYPE =
//boost::json::object; ~ const std::string ARG_AUTO_SRC_VERTEX_NAME =
//"autoSourceVertex"; ~ const std::string ARG_AUTO_SRC_VERTEX_DESC = "a JSON
//logic expression, describing how to compute the source vertex"; ~ const
//ARG_AUTO_SRC_VERTEX_KEY_TYPE ARG_AUTO_SRC_VERTEX_DFLT = {};

//~ using                              ARG_AUTO_SRC_VERTEX_TYPE =
//boost::json::object; ~ const std::string ARG_AUTO_SRC_VERTEX_NAME =
//"autoTargetVertex"; ~ const std::string ARG_AUTO_SRC_VERTEX_DESC = "a JSON
//logic expression, describing how to compute the target vertex"; ~ const
//ARG_AUTO_SRC_VERTEX_KEY_TYPE ARG_AUTO_SRC_VERTEX_DFLT = {};

}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");

  clip.add_required<ARG_EDGE_FILES_TYPE>(ARG_EDGE_FILES_NAME,
                                         ARG_EDGE_FILES_DESC);
  clip.add_optional<ARG_AUTO_VERTEX_TYPE>(
      ARG_AUTO_VERTEX_NAME, ARG_AUTO_VERTEX_DESC, ARG_AUTO_VERTEX_DFLT);
  //~ clip.add_optional<ARG_AUTO_SRC_VERTEX_TYPE>(ARG_AUTO_SRC_VERTEX_NAME,
  //ARG_AUTO_SRC_VERTEX_DESC, ARG_AUTO_SRC_VERTEX_DFLT); ~
  //clip.add_optional<ARG_AUTO_TGT_VERTEX_TYPE>(ARG_AUTO_TGT_VERTEX_NAME,
  //ARG_AUTO_TGT_VERTEX_DESC, ARG_AUTO_TGT_VERTEX_DFLT);

  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const ARG_EDGE_FILES_TYPE edgeFiles =
        clip.get<ARG_EDGE_FILES_TYPE>(ARG_EDGE_FILES_NAME);
    const ARG_AUTO_VERTEX_TYPE edgeVertexFields =
        clip.get<ARG_AUTO_VERTEX_TYPE>(ARG_AUTO_VERTEX_NAME);
    //~ const ARG_AUTO_SRC_VERTEX_TYPE edgeSrcLogic =
    //clip.get<ARG_AUTO_SRC_VERTEX_TYPE>(ARG_AUTO_SRC_VERTEX_NAME); ~ const
    //ARG_AUTO_SRC_VERTEX_TYPE edgeTgtLogic =
    //clip.get<ARG_AUTO_TGT_VERTEX_TYPE>(ARG_AUTO_TGT_VERTEX_NAME);
    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    metall_manager mm{metall::open_only, dataLocation.data(), MPI_COMM_WORLD};
    xpr::metall_graph             g{mm, world};
    std::vector<std::string_view> edgeVertexFieldsVw{edgeVertexFields.begin(),
                                                     edgeVertexFields.end()};
    const xpr::import_summary     summary =
        g.read_edge_files(edgeFiles, edgeVertexFieldsVw);

    if (world.rank() == 0) {
      clip.to_return(summary.asJson());
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
