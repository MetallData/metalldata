// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <clippy/clippy.hpp>
#include <boost/json.hpp>
#include <iostream>
#include <map>
#include <string>
#include "Graph.hpp"

namespace boostjsn = boost::json;

static const std::string method_name = "size";

int main(int argc, char **argv) {
  clippy::clippy clip{method_name, "Returns the number of vertices and edges."};
  clip.add_required_state<std::string>("path", "Path to the Metall storage.");
  clip.add_required_state<std::string>("key", "Name of the Graph object.");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv)) {
    return 0;
  }

  std::string path = clip.get_state<std::string>("path");
  std::string key  = clip.get_state<std::string>("key");

  clip.to_return(path);

  return 0;
}
