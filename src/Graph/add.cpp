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

static const std::string method_name = "add";

int main(int argc, char** argv) {
  ygm::comm      world(&argc, &argv);
  clippy::clippy clip{method_name, "Initializes a Graph"};
  clip.add_required<boostjsn::object>("selector", "Parent Selector");
  clip.add_required<std::string>("subname", "Description of new selector");
  clip.add_optional<std::string>("desc", "Description", "EMPTY DESCRIPTION");


  clip.add_required_state<std::string>("path", "Path to the Metall storage.");
  clip.add_required_state<std::string>("key", "Name of the Graph object.");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  std::string path = clip.get_state<std::string>("path");
  std::string key  = clip.get_state<std::string>("key");
  std::string selector_name = get_selector_name(clip.get<boostjsn::object>("selector"));
  std::string subname = clip.get<std::string>("subname");
  std::string desc = clip.get<std::string>("desc");

 {
    metall::utility::metall_mpi_adaptor mpi_adaptor(metall::open_only, path,
                                                    MPI_COMM_WORLD);
    auto& metall_manager = mpi_adaptor.get_local_manager();
    auto pgraph = metall_manager.find<Graph>(key.c_str());

    pgraph.first->add_meta( selector_name+"."+subname, desc);

    clip.update_selectors(pgraph.first->get_meta_map());
  }

  return 0;
}