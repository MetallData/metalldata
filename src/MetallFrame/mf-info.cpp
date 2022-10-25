// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements joining two MetallFrame data sets.

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>
#include <functional>
//~ #include <bit>

#include <boost/json.hpp>
#include <boost/functional/hash.hpp>
#include <metall/container/experimental/json/parse.hpp>

#include "mf-common.hpp"
#include "clippy/clippy-eval.hpp"


namespace bj      = boost::json;
namespace mtlutil = metall::utility;
namespace mtljsn  = metall::container::experimental::json;
namespace jl      = json_logic;


namespace
{
const std::string    methodName       = "info";


struct ProcessData
{
  std::vector<int>         rawresult;
};

ProcessData local;

struct InfoReduction
{
  std::vector<int> operator()(const std::vector<int>& lhs, const std::vector<int>& rhs) const
  {
    std::vector<int> res{lhs.begin(), lhs.end()};

    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
  }
};

}


int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Returns information about the vector storage."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv, world)) { return 0; }

  try
  {
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    mtlutil::metall_mpi_adaptor manager(metall::open_read_only, dataLocation.c_str(), MPI_COMM_WORLD);

    vector_json_type*           vec = &jsonVector(manager);

    int total    = vec->size();
    int selected = total;

    if (clip.has_state(ST_SELECTED))
    {
      selected = 0;
      forAllSelected( [&selected,vec](int, const vector_json_type::value_type&) -> void { ++selected; },
                      world.rank(),
                      *vec,
                      clip.get_state<JsonExpression>(ST_SELECTED)
                    );
    }

    //~ bj::object res;

    //~ res["rank"]     = world.rank();
    //~ res["total"]    = local.total;
    //~ res["selected"] = local.selected;
    std::vector<int> res = { int(world.rank()), int(total), int(selected) };

    res = world.all_reduce(res, InfoReduction{});

    world.barrier();

    if (world.rank() == 0)
    {
      bj::array arr;

      for (int i = 0; i < res.size(); i+=3)
      {
        bj::object obj;

        obj["rank"]  = res.at(i);
        obj["elements"] = res.at(i+1);
        obj["selected"] = res.at(i+2);

        arr.emplace_back(std::move(obj));
      }

      clip.to_return(arr);
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


