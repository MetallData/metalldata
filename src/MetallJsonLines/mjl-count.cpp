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

const std::string COUNT_ALL_NAME = "count_all";
const std::string COUNT_ALL_DESC = "if true, the selection criteria is ignored";
} // anonymous




int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Eval counts the number of rows where the current predicate(s) evaluate to true."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  clip.add_optional<bool>(COUNT_ALL_NAME, COUNT_ALL_DESC, false);

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::string    dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    const bool           countAll     = clip.get<bool>(COUNT_ALL_NAME);
    xpr::MetallJsonLines lines{world, metall::open_read_only, dataLocation, MPI_COMM_WORLD};
    const std::size_t    res          = countAll ? lines.count()
                                                 : lines.filter(filter(world.rank(), clip)).count();

    if (world.rank() == 0)
    {
      clip.to_return(res);
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}

