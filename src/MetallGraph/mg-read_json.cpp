// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mg-common.hpp"

namespace xpr     = experimental;

namespace
{
const std::string METHOD_NAME  = "read_json";
const std::string METHOD_DESC  = "Imports Json Data from files into the MetallJsonLines object.";

const std::string ARG_NODES_FILES_NAME = "node_files";
const std::string ARG_NODES_FILES_DESC = "A list of Json files that will be imported as nodes.";

const std::string ARG_EDGES_FILES_NAME = "edge_files";
const std::string ARG_EDGES_FILES_DESC = "A list of Json files that will be imported as edges.";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int             error_code = 0;
  clippy::clippy  clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");

  clip.add_optional<std::vector<std::string> >(ARG_NODES_FILES_NAME, ARG_NODES_FILES_DESC, {});
  clip.add_optional<std::vector<std::string> >(ARG_EDGES_FILES_NAME, ARG_EDGES_FILES_DESC, {});
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::vector<std::string> nodeFiles = clip.get<std::vector<std::string> >(ARG_NODES_FILES_NAME);
    const std::vector<std::string> edgeFiles = clip.get<std::vector<std::string> >(ARG_EDGES_FILES_NAME);
    const std::string              dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    xpr::MetallGraph               g{MPI_COMM_WORLD, world, metall::open_only, dataLocation};
    const std::size_t              numNodes = g.nodes().readJsonFiles(nodeFiles);
    const std::size_t              numEdges = g.edges().readJsonFiles(edgeFiles);

    if (world.rank() == 0)
    {
      if (nodeFiles.empty() || edgeFiles.empty())
      {
        // at least numNodes or numEdges will be 0.
        clip.to_return(numNodes+numEdges);
      }
      else
      {
        boost::json::object res;

        res["nodes"] = numNodes;
        res["edges"] = numEdges;

        clip.to_return(res);
      }
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


