// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements MetallGraph constructor (__init__).

#include "mg-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME      = "hist";
const std::string METHOD_DOCSTRING = "Make a histogram (currently only nodes)";
const std::string COLUMN_NAME      = "col";
}  // namespace

int ygm_main(ygm::comm &world, int argc, char **argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DOCSTRING};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");
  clip.add_required<std::string>(COLUMN_NAME, "Column name");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation =
        clip.get_state<std::string>(ST_METALL_LOCATION);
    const std::string colName = clip.get<std::string>(COLUMN_NAME);
    metall_manager    mm{metall::open_read_only, dataLocation.data(),
                      MPI_COMM_WORLD};
    xpr::metall_graph g{mm, world};
    const auto        res = g.nodes()
                         .filter(filter(world.rank(), clip, NODES_SELECTOR))
                         .hist(colName);

    if (world.rank() == 0) {
      clip.to_return(res);
    }
  } catch (const std::exception &err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  } catch (...) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return("unhandled, unknown exception");
  }

  return error_code;
}
