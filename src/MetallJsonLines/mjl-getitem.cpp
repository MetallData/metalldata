// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements MetallJsonLines selector function (getItem).

#include "mjl-common.hpp"

namespace {
const std::string METHOD_NAME = "__getitem__";
const std::string METHOD_DESC = "Sets the selector predicate(s).";

const parameter_description<JsonExpression> arg_expressions{"expressions", "Expression selection"};
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MJL_CLASS_NAME, "A " + MJL_CLASS_NAME + " class");

  arg_expressions.register_with_clippy(clip);

  clip.add_selector<std::string>(KEYS_SELECTOR, "Row selection key");
  clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                       "Metall storage location");

  // \note running on rank 0 suffices
  if ((world.rank() == 0) && clip.parse(argc, argv)) {
    return 0;
  }

  // the real thing
  try {
    if (world.rank() == 0) {
      std::string    location = clip.get_state<std::string>(ST_METALL_LOCATION);
      JsonExpression jsonExpression = arg_expressions.get(clip);
      JsonExpression selectedExpression;

      if (clip.has_state(ST_SELECTED))
        selectedExpression = clip.get_state<JsonExpression>(ST_SELECTED);

      append(selectedExpression, std::move(jsonExpression));

      clippy::object res;
      clippy::object clippy_type;
      clippy::object state;

      state.set_val(ST_METALL_LOCATION, std::move(location));
      state.set_val(ST_SELECTED, std::move(selectedExpression));

      clippy_type.set_val("__class__", MJL_CLASS_NAME);
      clippy_type.set_json("state", std::move(state));

      res.set_json("__clippy_type__", std::move(clippy_type));
      clip.to_return(std::move(res));
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
