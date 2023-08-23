// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements the construction of a metall_json_lines object.

#include "mjl-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "__init__";

const std::string METHOD_DOCSTRING =
    "Initializes a MetallJsonLines object\n"
    "creates a new physical object on disk "
    "only if it does not already exist.";

const std::string ARG_ALWAYS_CREATE_NAME = "overwrite";
const std::string ARG_ALWAYS_CREATE_DESC =
    "create new data store (deleting any existing data)";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DOCSTRING};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  clip.add_required<std::string>(ST_METALL_LOCATION,
                                 "Location of the Metall store");
  clip.add_optional<bool>(ARG_ALWAYS_CREATE_NAME, ARG_ALWAYS_CREATE_DESC,
                          false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    // the real thing
    // try to create the object
    std::string dataLocation = clip.get<std::string>(ST_METALL_LOCATION);
    const bool  overwrite    = clip.get<bool>(ARG_ALWAYS_CREATE_NAME);

    if (overwrite) remove_directory_and_content(world, dataLocation);

    if (!std::filesystem::is_directory(dataLocation)) {
      metall_manager mm{metall::create_only, dataLocation.data(),
                        MPI_COMM_WORLD};

      xpr::metall_json_lines::create_new(mm, world);
    } else {
      if (!metall::utility::metall_mpi_adaptor::consistent(dataLocation.data(),
                                                           MPI_COMM_WORLD))
        throw std::runtime_error{"Metallstore is inconsistent"};

      metall_manager mm{metall::open_read_only, dataLocation.data(),
                        MPI_COMM_WORLD};

      // check that storage is in consistent state
      xpr::metall_json_lines::check_state(mm, world);
    }

    world.barrier();

    // create the return object
    if (world.rank() == 0) {
      clip.set_state(ST_METALL_LOCATION, std::move(dataLocation));
    }
  } catch (const std::exception& ex) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(ex.what());
  } catch (...) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return("Unknown error");
  }

  return error_code;
}
