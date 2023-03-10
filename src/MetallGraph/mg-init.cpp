// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements MetallGraph constructor (__init__).

#include "mg-common.hpp"

namespace xpr     = experimental;

namespace
{
  const std::string METHOD_NAME = "__init__";
  const std::string METHOD_DOCSTRING = "Initializes a MetallGraph object\n"
                                       "creates a new physical object on disk "
                                       "only if it does not already exist.";

  const std::string ARG_ALWAYS_CREATE_NAME = "overwrite";
  const std::string ARG_ALWAYS_CREATE_DESC = "create new data store (deleting any existing data)";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DOCSTRING};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");

  clip.add_required<std::string>(ST_METALL_LOCATION, "Location of the Metall store");
  clip.add_optional<bool>(ARG_ALWAYS_CREATE_NAME, ARG_ALWAYS_CREATE_DESC, false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
  // the real thing
    // try to create the object
    std::string      dataLocation = clip.get<std::string>(ST_METALL_LOCATION);
    const bool       overwrite    = clip.get<bool>(ARG_ALWAYS_CREATE_NAME);
    auto             graphCreator = overwrite ? &xpr::MetallGraph::createOverwrite
                                              : &xpr::MetallGraph::createNewOnly;
 /* xpr::MetallGraph lines = */ graphCreator(MPI_COMM_WORLD, world, dataLocation);

    world.barrier();

    // create the return object
    if (world.rank() == 0)
    {
      clip.set_state(ST_METALL_LOCATION, std::move(dataLocation));
    }
  }
  catch (const std::exception& ex)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(ex.what());
  }
  catch (...)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return("Unknown error");
  }

  return error_code;
}


