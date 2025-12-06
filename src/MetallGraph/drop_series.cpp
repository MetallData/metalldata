// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metall_graph.hpp>

static const std::string method_name    = "drop_series";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Drops a series from a MetallGraph"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("series_name", "The name of the series.");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path        = clip.get_state<std::string>("path");
  auto series_name = clip.get<std::string>("series_name");

  metalldata::metall_graph mg(comm, path, false);

  if (!mg.has_series(series_name)) {
    comm.cerr0("Series name ", series_name, " not found; aborting");
    return 1;
  }

  mg.drop_series(series_name);
  clip.update_selectors(mg.get_selector_info());

  return 0;
}