// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements head function to return N entries from a MetallJsoneLines

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>
//~ #include <ranges>

#include <boost/json.hpp>

#include "mjl-common.hpp"
#include "clippy/clippy-eval.hpp"

namespace xpr     = experimental;

namespace
{

const std::string    methodName      = "head";
const std::string    ARG_MAX_ROWS    = "num";
const std::string    COLUMNS         = "columns";
const ColumnSelector DEFAULT_COLUMNS = {};

} // anonymous



int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Returns n arbitrary rows for which the predicate evaluates to true."};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  clip.add_optional<int>(ARG_MAX_ROWS, "Max number of rows returned", 5);
  clip.add_optional<ColumnSelector>(COLUMNS, "projection list (list of columns to put out)", DEFAULT_COLUMNS);
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    const std::string    dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    const std::size_t    numrows      = clip.get<int>(ARG_MAX_ROWS);
    xpr::MetallJsonLines lines{MPI_COMM_WORLD, world, metall::open_read_only, dataLocation};
    boost::json::value   res          = lines.filter(filter(world.rank(), clip, SELECTOR))
                                             .head(numrows, projector(COLUMNS, clip));

    if (world.rank() == 0) clip.to_return(std::move(res));
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}

