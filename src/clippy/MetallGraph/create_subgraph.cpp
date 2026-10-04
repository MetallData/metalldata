// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>

static const std::string method_name = "create_subgraph";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{
    method_name,
    "Creates a named subgraph from the nodes and edges selected by the where "
    "clause"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("name", "subgraph name to create");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");
  auto name = clip.get<std::string>("name");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  auto result = mg.create_subgraph(name, where_c);
  if (!result) {
    comm.cerr0(result.error());
    return 1;
  }
  clip.update_selectors(mg.get_selector_info());
  return 0;
} catch (const std::runtime_error& e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
