// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements MetallGraph constructor (__init__).

#include "mg-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "count";
const std::string METHOD_DOCSTRING =
    "Counts the number of rows where the current selection criteria is true. "
    "Edges are counted only if their endpoints are both in the counted "
    "vertices set.";
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

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    metall_manager        mm{metall::open_read_only, dataLocation.data(),
                      MPI_COMM_WORLD};
    xpr::metall_graph     g{mm, world};
    xpr::mg_count_summary res =
        g.count(filter(world.rank(), clip, NODES_SELECTOR),
                filter(world.rank(), clip, EDGES_SELECTOR));

    if (world.rank() == 0) {
      clip.to_return(res.asJson());
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
