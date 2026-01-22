// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include "utils.hpp"

static const std::string method_name    = "rename_series";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

using series_name = metalldata::metall_graph::series_name;

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Renames a series in a MetallGraph"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<boost::json::object>("old_name", "The series to rename.");

  clip.add_required<std::string>("new_name", "The new name of the series.");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path         = clip.get_state<std::string>("path");
  auto old_name_obj = clip.get<boost::json::object>("old_name");

  auto try_old_name = metalldata::obj2sn(old_name_obj);
  if (!try_old_name.has_value()) {
    comm.cerr0("Series name invalid; aborting");
    return 1;
  }
  series_name old_name = try_old_name.value();

  auto new_name_str = clip.get<std::string>("new_name");
  auto new_name     = series_name(new_name_str);

  if (!new_name.is_qualified()) {
    new_name = series_name(old_name.prefix(), new_name.unqualified());
  }

  metalldata::metall_graph mg(comm, path, false);

  auto result = mg.rename_series(old_name, new_name);
  if (!result.has_value()) {
    comm.cerr0(result.error());
    return 1;
  }
  if (!result.value()) {
    comm.cerr0("Rename failed");
    return 1;
  }
  clip.update_selectors(mg.get_selector_info());
  return 0;
}