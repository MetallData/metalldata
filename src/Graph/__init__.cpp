// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT
#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <boost/json.hpp>
#include <iostream>
#include <map>
#include <string>
#include "Graph.hpp"

#include <metall/utility/metall_mpi_adaptor.hpp>
#include <metall/utility/filesystem.hpp>

namespace boostjsn = boost::json;

static const std::string method_name = "__init__";

int main(int argc, char** argv) {
  ygm::comm      world(&argc, &argv);
  clippy::clippy clip{method_name, "Initializes a Graph"};

  clip.add_required<std::string>("path", "Path to Metall storage on backend.");
  clip.add_required<std::string>("key", "Name of the Graph object.");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  std::string path = clip.get<std::string>("path");
  std::string key  = clip.get<std::string>("key");

  clip.set_state("path", path);
  clip.set_state("key", key);

  static bool metall_exists = false;
  if (world.rank0()) {
    bool exists = ::std::filesystem::exists(path);
    world.async_bcast([](bool flag) { metall_exists = flag; }, exists);
  }

  world.barrier();

  if (metall_exists) {
    metall::utility::metall_mpi_adaptor mpi_adaptor(metall::open_only, path,
                                                    MPI_COMM_WORLD);
    auto& metall_manager = mpi_adaptor.get_local_manager();
    auto pgraph = metall_manager.find<Graph>(key.c_str());

  } else {
    metall::utility::metall_mpi_adaptor mpi_adaptor(metall::create_only, path,
                                                    MPI_COMM_WORLD);
    auto& metall_manager = mpi_adaptor.get_local_manager();
    using namespace metall::container::experimental::string_container;

    auto *main_table = metall_manager.construct<string_table<>>(
        metall::unique_instance)(metall_manager.get_allocator<>());

    auto pgraph = metall_manager.construct<Graph>(key.c_str())(metall_manager.get_allocator(),main_table);  
  }

  return 0;
}