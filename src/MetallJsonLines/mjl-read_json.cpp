// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "mjl-common.hpp"

namespace xpr     = experimental;

namespace
{
const std::string ARG_IMPORTED = "Json file";
const std::string METHOD_NAME  = "read_json";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int             error_code = 0;
  clippy::clippy  clip{METHOD_NAME, "Imports Json Data from files into the MetallJsonLines object."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_required<std::vector<std::string> >(ARG_IMPORTED, "Json files to be ingested.");
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::vector<std::string> files = clip.get<std::vector<std::string> >(ARG_IMPORTED);
    const std::string              dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    xpr::MetallJsonLines           lines{world, metall::open_only, dataLocation, MPI_COMM_WORLD};
    const std::size_t              totalImported = lines.readJsonFiles(files);

    if (world.rank() == 0)
    {
      clip.to_return(totalImported);
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


