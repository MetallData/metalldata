// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements MetallGraph selector function (getItem).

#include "mg-common.hpp"

namespace xpr     = experimental;

namespace
{
const std::string methodName = "__getitem__";
const std::string expr = "expressions";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "Sets the selector predicate(s)."};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");

  clip.add_required<std::vector<boost::json::object>>(expr, "Expression selection");
  clip.add_selector<std::string>(SELECTOR, "Row selection predicate");
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  // \note running on rank 0 suffices
  if ((world.rank() == 0) && clip.parse(argc, argv)) { return 0; }

  // the real thing
  try
  {
    if (world.rank() == 0)
    {
      std::string    location = clip.get_state<std::string>(ST_METALL_LOCATION);
      JsonExpression jsonExpression = clip.get<JsonExpression>(expr);
      JsonExpression selectedExpression;

      if (clip.has_state(ST_SELECTED))
        selectedExpression = clip.get_state<JsonExpression>(ST_SELECTED);

      append(selectedExpression, std::move(jsonExpression));

      clippy::object res;
      clippy::object clippy_type;
      clippy::object state;

      state.set_val(ST_METALL_LOCATION, std::move(location));
      state.set_val(ST_SELECTED,        std::move(selectedExpression));

      clippy_type.set_val("__class__", MG_CLASS_NAME);
      clippy_type.set_json("state",    std::move(state));

      res.set_json("__clippy_type__",  std::move(clippy_type));
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

