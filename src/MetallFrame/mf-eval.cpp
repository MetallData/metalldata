// Copyright 2022 Lawrence Livermore National Security, LLC and other CLIPPy Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Demonstrates how to run Json expression predicates on a JsonFrame

#include <iostream>
#include <fstream>

#include <boost/json.hpp>
#include "mf-common.hpp"


namespace bjsn    = boost::json;
namespace jl      = json_logic;
namespace mtlutil = metall::utility;

namespace
{
const std::string methodName = "eval";
}




int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Eval counts the number of rows where the current predicate(s) evaluate to true."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv)) { return 0; }

  try
  {
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    mtlutil::metall_mpi_adaptor manager(metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD);
    vector_json_type&           vec = jsonVector(manager);
    int                         numSelected = vec.size();

    if (clip.has_state(ST_SELECTED))
    {
      JsonExpression   jsonExpression = clip.get_state<JsonExpression>(ST_SELECTED);
      std::vector<int> selectedRows = computeSelected(vec, jsonExpression);

      numSelected = selectedRows.size();
    }

    world.barrier(); // necessary?

    int totalSelected = world.all_reduce_sum(numSelected);

    if (world.rank() == 0)
    {
      std::stringstream msg;

      msg << "Selected " << totalSelected << " rows." << std::flush;
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

