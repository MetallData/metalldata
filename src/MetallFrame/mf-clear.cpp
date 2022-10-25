// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mf-common.hpp"

namespace mtlutil = metall::utility;
namespace mtljsn  = metall::container::experimental::json;

namespace
{
const std::string METHOD_NAME  = "clear";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int             error_code = 0;
  clippy::clippy  clip{METHOD_NAME, "Erases all elements in the MetallFrame."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    mtlutil::metall_mpi_adaptor manager(metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD);
    vector_json_type*           vec = &jsonVector(manager);

    assert(vec);
    vec->clear();

    assert(vec->size() == size_t{0});

    if (world.rank() == 0)
    {
      std::stringstream msg;

      msg << "all rows deleted." << std::flush;
      clip.to_return(msg.str());
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


