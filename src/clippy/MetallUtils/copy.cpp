// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <mpi.h>
#include <stdexcept>
#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <filesystem>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <limits>

static const std::string method_name = "copy";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Copies Metall storage across processors"};
  clip.add_required<std::string>("srcpath", "Path to source Metall storage");
  clip.add_required<std::string>("dstpath",
                                 "Path to destination Metall storage");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto src_path = clip.get<std::string>("srcpath");
  auto dst_path = clip.get<std::string>("dstpath");

  if (comm.rank0() && !std::filesystem::exists(src_path)) {
    std::cerr << "Source path does not exist: " << src_path << std::endl;
    return 1;
  }

  metall::utility::metall_mpi_adaptor::copy(src_path, dst_path,
                                            comm.get_mpi_comm());
  return 0;
} catch (std::runtime_error e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}