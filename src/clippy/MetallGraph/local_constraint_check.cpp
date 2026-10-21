// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include "utils.hpp"

static const std::string method_name = "local_constraint_check";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{
    method_name,
    "PruneJuice local constraint checking: marks edges that may participate in "
    "a match of the template"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("output", "Output edge series name");
  clip.add_required<boost::json::object>(
    "template",
    "Template: {\"edges\": [[u, v], [u, v, predicate], ...], \"nodes\": "
    "{name: predicate, ...}}; \"nodes\" is optional");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto output = clip.get<std::string>("output");
  auto tmpl_obj = clip.get<boost::json::object>("template");
  auto where = clip.get<boost::json::object>("where");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph::series_name sname(output);
  if (sname.prefix().empty()) {
    sname = metalldata::metall_graph::series_name("edge", output);
  }
  if (!sname.is_edge_series()) {
    comm.cerr0("Invalid edge series name: ", sname.qualified());
    return -1;
  }

  auto tmpl = metalldata::parse_pattern_template(tmpl_obj);
  if (!tmpl) {
    comm.cerr0(tmpl.error());
    return -1;
  }

  metalldata::metall_graph mg(comm, path, false);
  auto rc = mg.local_constraint_check(sname, tmpl.value(), where_c);

  if (!rc) {
    comm.cerr0(rc.error());
    return -1;
  }

  for (const auto &[warn, count] : rc.warnings()) {
    comm.cerr0(std::format("{} : {}", warn, count));
  }

  std::map<std::string, size_t> stats{{"rounds", rc.value().rounds},
                                      {"num_nodes", rc.value().num_nodes},
                                      {"num_edges", rc.value().num_edges}};
  clip.to_return(stats);
  clip.update_selectors(mg.get_selector_info());
  return 0;
} catch (const std::runtime_error &e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
