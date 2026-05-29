// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <filesystem>

static const std::string method_name = "remove";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Removes Metall storage across processors"};
  clip.add_required<std::string>("path", "Path to Metall storage");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get<std::string>("path");

  if (comm.layout().local_id() == 0) {
    // Only one rank per node needs to call remove_all
    std::filesystem::remove_all(path);
  }

  return 0;
} catch (std::runtime_error e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
