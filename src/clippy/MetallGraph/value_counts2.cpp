// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <stdexcept>
#include <utility>
#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include "utils.hpp"

static const std::string method_name = "value_counts2";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char **argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name,
                      "Returns the 2nd order of the value counts for a series "
                      "in a MetallGraph (value_counts of value_counts)"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");

  clip.add_required<boost::json::object>("series_name", "Series name to use");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});
  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto sn_obj = clip.get<boost::json::object>("series_name");
  auto where = clip.get<boost::json::object>("where");

  auto try_sn = metalldata::obj2sn(sn_obj);
  if (!try_sn.has_value()) {
    comm.cerr0("Series name invalid; aborting");
    return 1;
  }
  metalldata::metall_graph::series_name sn = try_sn.value();

  metalldata::metall_graph mg(comm, path, false);

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
    // comm.cerr("Found RULE");
  }

  auto counts = mg.value_counts2(sn, where_c);

  std::map<size_t, size_t> gathered_counts;
  counts.gather(gathered_counts, 0);

  clip.to_return(gathered_counts);
  return 0;
} catch (const std::runtime_error &e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.";
}