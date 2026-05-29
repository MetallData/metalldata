// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>

static const std::string method_name    = "welcome";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Prints YGM's welcome message"};

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  comm.welcome(std::cerr);

  return 0;
} catch (std::runtime_error e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
