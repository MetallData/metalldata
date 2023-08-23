// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Demonstrates how to run Json expression predicates on a MetallFrame

#include <fstream>
#include <iostream>

#include <boost/json.hpp>
#include "df-common.hpp"

namespace bjsn = boost::json;
namespace jl   = json_logic;
namespace xpr  = experimental;

namespace {
const std::string methodName = "count";

struct ProcessData {
  vector_json_type* vec      = nullptr;
  std::uint64_t     selected = 0;
};

ProcessData local;

}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{methodName,
                      "Eval counts the number of rows where the current "
                      "predicate(s) evaluate to true."};

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

    local.vec      = dfp.get();
    local.selected = local.vec->rows();

    if (clip.has_state(ST_SELECTED)) {
      local.selected = 0;
      forAllSelected([](int) -> void { ++local.selected; }, world.rank(),
                     *local.vec, clip.get_state<JsonExpression>(ST_SELECTED));
    }

    world.barrier();  // necessary?

    int totalSelected = world.all_reduce_sum(local.selected);

    if (world.rank() == 0) {
      std::stringstream msg;

      msg << "Selected " << totalSelected << " rows." << std::flush;
      clip.to_return(msg.str());
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
