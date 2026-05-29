// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>

static const std::string method_name    = "__init__";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Initializes a MetallGraph"};
  clip.add_required<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<bool>("overwrite", "Overwrite existing storeage", false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path      = clip.get<std::string>("path");
  auto overwrite = clip.get<bool>("overwrite");

  clip.set_state("path", path);

  metalldata::metall_graph mg(comm, path, overwrite);
  clip.update_selectors(mg.get_selector_info());
  return 0;
} catch (std::runtime_error e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
