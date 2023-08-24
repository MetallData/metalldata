// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements MetallGraph constructor (__init__).

#include "mg-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "count_lines";
const std::string METHOD_DOCSTRING =
    "Counts the number of rows where the current selection criteria is true.";

const std::string COUNT_ALL_NAME = "count_all";
const std::string COUNT_ALL_DESC = "if true, the selection criteria is ignored";

const std::string WO_NODES_NAME = "without_nodes";
const std::string WO_NODES_DESC = "if true, nodes are not counted";

const std::string WO_EDGES_NAME = "without_edges";
const std::string WO_EDGES_DESC = "if true, edges are not counted";
}  // namespace

std::size_t countLines(bool skip, bool ignoreFilter,
                       xpr::metall_json_lines& lines, std::size_t rank,
                       clippy::clippy& clip, std::string_view selector) {
  if (skip) return 0;
  if (ignoreFilter) return lines.count();

  return lines.filter(filter(rank, clip, selector)).count();
}

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DOCSTRING};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  clip.add_optional<bool>(COUNT_ALL_NAME, COUNT_ALL_DESC, false);
  clip.add_optional<bool>(WO_NODES_NAME, WO_NODES_DESC, false);
  clip.add_optional<bool>(WO_EDGES_NAME, WO_EDGES_DESC, false);

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    const bool        countAll     = clip.get<bool>(COUNT_ALL_NAME);
    const bool        withoutNodes = clip.get<bool>(WO_NODES_NAME);
    const bool        withoutEdges = clip.get<bool>(WO_EDGES_NAME);
    metall_manager    mm{metall::open_read_only, dataLocation.data(),
                      MPI_COMM_WORLD};
    xpr::metall_graph g{mm, world};
    const std::size_t numNodes = countLines(withoutNodes, countAll, g.nodes(),
                                            world.rank(), clip, NODES_SELECTOR);
    const std::size_t numEdges = countLines(withoutEdges, countAll, g.edges(),
                                            world.rank(), clip, EDGES_SELECTOR);

    if (world.rank() == 0) {
      if (withoutNodes || withoutEdges) {
        // at least numNodes or numEdges will be 0.
        clip.to_return(numNodes + numEdges);
      } else {
        boost::json::object res;

        res["nodes"] = numNodes;
        res["edges"] = numEdges;

        clip.to_return(res);
      }
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
