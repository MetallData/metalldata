// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include "metall_graph.hpp"

static const std::string method_name    = "__init__";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Initializes a MetallGraph"};
  clip.add_required<std::string>("path", "Storage path for MetallGraph");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  std::string path = clip.get<std::string>("path");
  clip.set_state("path", path);

  metalldata::metall_graph test(comm, path);

  return 0;
}