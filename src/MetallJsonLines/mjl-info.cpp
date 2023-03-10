// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements joining two MetallJsonLines data sets.

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>
#include <functional>
//~ #include <bit>

#include <boost/json.hpp>
#include <boost/functional/hash.hpp>
#include <metall/container/experimental/json/parse.hpp>

#include "mjl-common.hpp"
#include "clippy/clippy-eval.hpp"

namespace xpr     = experimental;

namespace
{
const std::string    methodName       = "info";
}


int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Returns information about the vector storage."};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::string    dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    xpr::MetallJsonLines lines{MPI_COMM_WORLD, world, metall::open_read_only, dataLocation};
    boost::json::value   res          = lines.filter(filter(world.rank(), clip, SELECTOR))
                                             .info();

    if (world.rank() == 0) clip.to_return(std::move(res));
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


