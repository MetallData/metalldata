// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  metalldata::metall_graph test(world, "dummy");
  bool                     x =
    test.has_series(metalldata::metall_graph::series_name("edge.series1"));
  world.cerr0("has_series 1 is ", x);
  x = test.add_series<int64_t>(
    metalldata::metall_graph::series_name("edge.series1"));
  world.cerr0("add_series is ", x);
  x = test.has_series(metalldata::metall_graph::series_name("edge.series1"));
  world.cerr0("has_series 2 is ", x);
}