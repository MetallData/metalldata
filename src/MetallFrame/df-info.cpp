// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements joining two MetallFrame data sets.

#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
//~ #include <bit>

#include <boost/json.hpp>

#include "clippy/clippy-eval.hpp"
#include "df-common.hpp"

namespace bj  = boost::json;
namespace jl  = json_logic;
namespace xpr = experimental;

namespace {
const std::string methodName = "info";

struct ProcessData {
  using count_t  = std::size_t;
  using vector_t = std::vector<count_t>;

  vector_t rawresult;
};

ProcessData local;

struct InfoReduction {
  ProcessData::vector_t operator()(const ProcessData::vector_t& lhs,
                                   const ProcessData::vector_t& rhs) const {
    ProcessData::vector_t res{lhs.begin(), lhs.end()};

    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
  }
};

}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{methodName,
                      "Returns information about the vector storage."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");
  clip.add_required_state<std::string>(ST_METALLFRAME_NAME, "Metallframe2 key");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    std::string location = clip.get_state<std::string>(ST_METALL_LOCATION);
    std::string key      = clip.get_state<std::string>(ST_METALLFRAME_NAME);
    std::unique_ptr<xpr::DataFrame> dfp =
        makeDataFrame(false /* existing */, location, key);

    ProcessData::count_t total    = dfp->rows();
    ProcessData::count_t selected = total;

    if (clip.has_state(ST_SELECTED)) {
      selected = 0;
      forAllSelected([&selected](std::int64_t) -> void { ++selected; },
                     world.rank(), *dfp,
                     clip.get_state<JsonExpression>(ST_SELECTED));
    }

    ProcessData::vector_t res = {ProcessData::count_t(world.rank()), total,
                                 selected};

    res = world.all_reduce(res, InfoReduction{});

    world.barrier();

    if (world.rank() == 0) {
      bj::array arr;

      for (int i = 0; i < res.size(); i += 3) {
        bj::object obj;

        obj["rank"]     = res.at(i);
        obj["elements"] = res.at(i + 1);
        obj["selected"] = res.at(i + 2);

        arr.emplace_back(std::move(obj));
      }

      clip.to_return(arr);
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
