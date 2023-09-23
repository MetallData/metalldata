// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <iostream>

#include "ygm/comm.hpp"

#include "MetallGraph.hpp"

namespace xpr = experimental;

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  std::vector<std::string> edgeFiles;

  try {
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation = "/tmp/metall";
    metall_manager mm{metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD};
    xpr::metall_graph             g{mm, comm};
    std::vector<std::string_view> edgeVertexFieldsVw{};

    const xpr::import_summary summary = g.read_edge_files(
        edgeFiles, xpr::metall_graph::file_type::json, edgeVertexFieldsVw);
    comm.cout0() << summary.asJson() << std::endl;

  } catch (const std::exception& err) {
    std::cerr << err.what() << std::endl;
  }

  return 0;
}