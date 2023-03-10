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
const std::string METHOD_NAME  = "read_json";
const std::string METHOD_DESC  = "Imports Json Data from files into the MetallJsonLines object.";

const std::string ARG_JSON_FILES_NAME = "json_files";
const std::string ARG_JSON_FILES_DESC = "A list of Json files that will be imported.";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int             error_code = 0;
  clippy::clippy  clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  clip.add_required<std::vector<std::string> >(ARG_JSON_FILES_NAME, ARG_JSON_FILES_DESC);
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::vector<std::string> files = clip.get<std::vector<std::string> >(ARG_JSON_FILES_NAME);
    const std::string              dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    xpr::MetallJsonLines           lines{MPI_COMM_WORLD, world, metall::open_only, dataLocation};
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


