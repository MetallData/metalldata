// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Demonstrates how to run Json expression predicates on a MetallJsonLines

#include <iostream>
#include <fstream>

#include <boost/json.hpp>
#include "mjl-common.hpp"

namespace xpr     = experimental;

namespace
{
const std::string methodName = "count";
} // anonymous




int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Eval counts the number of rows where the current predicate(s) evaluate to true."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::string    dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    xpr::MetallJsonLines lines{world, metall::open_read_only, dataLocation, MPI_COMM_WORLD};
    const std::size_t    res          = lines.filter(filter(world.rank(), clip))
                                             .count();

    if (world.rank() == 0)
    {
      std::stringstream msg;

      msg << "Selected " << res << " rows." << std::flush;
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

