// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>

static const std::string method_name = "describe";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Provides basic graph statistics"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  metalldata::result<metalldata::metall_graph::graph_stats> statsres =
    mg.describe(where_c);

  std::map<std::string,
           std::variant<size_t, std::string, std::map<std::string, size_t>>>
    return_dict;
  if (statsres.has_value()) {
    auto stats = statsres.value();
    return_dict["nv"] = stats.nv;
    return_dict["ne"] = stats.ne;
    std::map<std::string, size_t> nonnull_edge_ct;
    std::map<std::string, size_t> nonnull_node_ct;

    for (const auto& [sname, ct] : stats.non_null_node_ct) {
      nonnull_node_ct[sname.qualified()] = ct;
    }
    return_dict["nonnull_node_count"] = nonnull_node_ct;
    for (const auto& [sname, ct] : stats.non_null_edge_ct) {
      nonnull_edge_ct[sname.qualified()] = ct;
    }
    return_dict["nonnull_edge_count"] = nonnull_edge_ct;
  }
  return_dict["path"] = path;

  clip.to_return(return_dict);
  // clip.to_return(std::make_pair(nv, ne));

  return 0;
} catch (const std::runtime_error& e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
