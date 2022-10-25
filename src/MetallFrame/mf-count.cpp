// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Demonstrates how to run Json expression predicates on a MetallFrame

#include <iostream>
#include <fstream>

#include <boost/json.hpp>
#include "mf-common.hpp"


namespace bjsn    = boost::json;
namespace jl      = json_logic;
namespace mtlutil = metall::utility;

namespace
{
const std::string methodName = "count";

struct ProcessData
{
  vector_json_type* vec = nullptr;
  std::uint64_t     selected = 0;
};

ProcessData local;

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
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    mtlutil::metall_mpi_adaptor manager(metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD);

    local.vec      = &jsonVector(manager);
    local.selected = local.vec->size();

    if (clip.has_state(ST_SELECTED))
    {
      local.selected = 0;
      forAllSelected( [](int, const vector_json_type::value_type&) -> void { ++local.selected; },
                      world.rank(),
                      *local.vec,
                      clip.get_state<JsonExpression>(ST_SELECTED)
                    );
    }

    world.barrier(); // necessary?

    int totalSelected = world.all_reduce_sum(local.selected);

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

