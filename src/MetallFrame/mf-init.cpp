// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements the construction of a MetallFrame object.

#include "mf-common.hpp"

namespace mtlutil = metall::utility;

namespace
{
  const std::string METHOD_NAME = "__init__";

  const std::string METHOD_DOCSTRING = "Initializes a MetallFrame object\n"
                                     "creates a new physical object on disk "
                                     "only if it does not already exist.";

  const std::string ARG_ALWAYS_CREATE_NAME = "overwrite";
  const std::string ARG_ALWAYS_CREATE_DESC = "create new data store (deleting any existing data)";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DOCSTRING};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_required<std::string>(ST_METALL_LOCATION, "Location of the Metall store");
  clip.add_optional<bool>(ARG_ALWAYS_CREATE_NAME, ARG_ALWAYS_CREATE_DESC, false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
  // the real thing
    // try to create the object
    std::string dataLocation = clip.get<std::string>(ST_METALL_LOCATION);
    const bool  overwrite    = clip.get<bool>(ARG_ALWAYS_CREATE_NAME);

    if (overwrite)
      if (std::filesystem::is_directory(dataLocation.c_str()))
        std::filesystem::remove_all(dataLocation.c_str());

    // only create a new container if specified explicitly, or if the path does not exist
    if (!mtlutil::metall_mpi_adaptor::consistent(dataLocation.c_str(), MPI_COMM_WORLD))
    {
      mtlutil::metall_mpi_adaptor manager(metall::create_only, dataLocation.c_str(), MPI_COMM_WORLD);
      auto&                       mgr = manager.get_local_manager();
      const auto*                 vec = mgr.construct<vector_json_type>(metall::unique_instance)(mgr.get_allocator());

      if (vec == nullptr)
        throw std::runtime_error("unable to allocate a MetallFrame-Object in Metall");
    }

    world.barrier();

    // create the return object
    if (world.rank() == 0)
    {
      clip.set_state(ST_METALL_LOCATION, std::move(dataLocation));
    }
  }
  catch (const std::runtime_error& ex)
  {
    error_code = 1;
    //~ if (world.rank() == 0) clip.to_return(ex.what());
  }
  catch (...)
  {
    error_code = 1;
    //~ if (world.rank() == 0) clip.to_return(ex.what());
  }

  return error_code;
}


