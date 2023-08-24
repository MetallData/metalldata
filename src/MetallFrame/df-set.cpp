// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>

#include <boost/json.hpp>

#include "clippy/clippy-eval.hpp"
#include "df-common.hpp"
#include "experimental/json-io.hpp"

namespace bjsn = boost::json;
namespace jl   = json_logic;
namespace xpr  = experimental;

namespace {
const std::string methodName     = "set";
const std::string ARG_COLUMN     = "column";
const std::string ARG_EXPRESSION = "expression";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{
      methodName, "For all selected rows, set a field to a (computed) value."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");
  clip.add_required_state<std::string>(ST_METALLFRAME_NAME, "Metallframe2 key");

  clip.add_required<std::string>(ARG_COLUMN, "output column");
  clip.add_required<bjsn::object>(ARG_EXPRESSION, "output value expression");

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    std::string  location   = clip.get_state<std::string>(ST_METALL_LOCATION);
    std::string  key        = clip.get_state<std::string>(ST_METALLFRAME_NAME);
    std::string  columnName = clip.get<std::string>(ARG_COLUMN);
    bjsn::object columnExpr = clip.get<bjsn::object>(ARG_EXPRESSION);
    std::unique_ptr<xpr::DataFrame> dfp =
        makeDataFrame(false /* existing */, location, key);
    auto [ast, vars, hasComputedVarNames] =
        jl::translateNode(columnExpr["rule"]);

    if (hasComputedVarNames)
      throw std::runtime_error("unable to work with computed variable names");

    int                       updCount = 0;
    const int                 rank     = world.rank();
    xpr::DataFrame&           dataset  = *dfp;
    const xpr::ColumnVariant& colaccess =
        dfp->get_column_variant_std(columnName);
    auto updateFn = [&dataset, &updCount, &ast, &colaccess,
                     rank](std::int64_t row) -> void {
      ++updCount;

      const std::int64_t selLen = (SELECTOR.size() + 1);

      auto varLookup = [&dataset, selLen, row, rank](
                           const boost::json::string& colname,
                           int) -> json_logic::ValueExpr {
        // \todo match selector instead of skipping it
        std::string_view col{colname.begin() + selLen, colname.size() - selLen};

        try {
          return toValueExpr(dataset.get_cell_variant(row, col));
        } catch (const experimental::unknown_column_error&) {
          if (col == "rowid") return json_logic::toValueExpr(row);
          if (col == "mpiid")
            return json_logic::toValueExpr(std::int64_t(rank));

          return json_logic::toValueExpr(nullptr);
        }
      };

      jl::ValueExpr exp = jl::calculate(ast, varLookup);

      xpr::setCellValue(dataset, colaccess, row, std::move(exp));
    };

    if (!clip.has_state(ST_SELECTED)) {
      for (int row = 0, lim = dataset.rows(); row < lim; ++row) updateFn(row);
    } else
      forAllSelected(updateFn, rank, *dfp,
                     clip.get_state<JsonExpression>(ST_SELECTED));

    world.barrier();  // necessary?

    const int totalUpdated = world.all_reduce_sum(updCount);

    if (world.rank() == 0) {
      std::stringstream msg;

      msg << "updated column " << columnName << " in " << totalUpdated
          << " entries" << std::endl;
      clip.to_return(msg.str());
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
