// Copyright 2022 Lawrence Livermore National Security, LLC and other CLIPPy Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements head function to return N entries from a JsonFrame.

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>

#include <boost/json.hpp>
#include "clippy/clippy.hpp"
#include "clippy/clippy-eval.hpp"
#include "mf-common.hpp"

namespace bjsn    = boost::json;
namespace mtljsn  = metall::container::experimental::json;
namespace mtlutil = metall::utility;
namespace jl      = json_logic;

namespace
{

static const std::string methodName = "head";
static const std::string ARG_MAX_ROWS = "num";

struct ProcessData
{
  vector_json_type*        vec = nullptr;
  std::vector<int>         selectedRows;
  std::vector<std::string> remoteRows;
};

ProcessData local;
}


struct RowResponse
{
  void operator()(std::vector<std::string> rows)
  {
    for (std::string& el : rows)
      local.remoteRows.emplace_back(std::move(el));
  }
};


struct RowRequest
{
  //~ template <class CommT>
  //~ void operator()(CommT* world, int numrows) const
  void operator()(ygm::comm* w, int numrows) const
  {
    assert(w);

    ygm::comm& world     = *w;
    const int  fromThis  = std::min(int(local.selectedRows.size()), numrows);
    const int  fromOther = numrows - fromThis;

    if ((fromOther > 0) && (world.size() != (world.rank()+1)))
      world.async( world.rank()+1, RowRequest{}, fromOther );

    for (int i = 0; i < fromThis; ++i)
    {
      std::stringstream serial;

      serial << local.vec->at(local.selectedRows.at(i)) << std::flush;

      local.remoteRows.emplace_back(serial.str());
    }

    world.async(0, RowResponse{}, local.remoteRows);
  }
};

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Returns n arbitrary rows for which the predicate evaluates to true."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_optional<int>(ARG_MAX_ROWS, "Max number of rows returned", 5);
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv)) { return 0; }

  try
  {
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    const int                   numrows      = clip.get<int>(ARG_MAX_ROWS);
    //~ std::string      key = clip.get_state<std::string>(ST_JSONLINES_KEY);
    mtlutil::metall_mpi_adaptor manager(metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD);

    local.vec = &jsonVector(manager);
    local.selectedRows = getSelectedRows(clip, *local.vec, numrows);
    world.barrier();

    const int                   numSelectedRows = local.selectedRows.size();

    if ((numSelectedRows < numrows) && (world.rank() == 0) && (world.size() != (world.rank()+1)))
      world.async( world.rank()+1, RowRequest{}, (numrows-numSelectedRows) );

    // maybe a variant of getSelectedRows can fill in the rows
    std::vector<bjsn::value>    res;

    if (world.rank() == 0)
    {
      for (int i : local.selectedRows)
        res.emplace_back(mtljsn::value_to<bjsn::value>(local.vec->at(i)));
    }

    world.barrier();

    if (world.rank() == 0)
    {
      for (const std::string& row : local.remoteRows)
        res.emplace_back(boost::json::parse(row));

      clip.to_return(std::move(res));
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}

