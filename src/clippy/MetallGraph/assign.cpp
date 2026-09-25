// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>

static const std::string method_name = "assign";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name,
                      "Creates a series and assigns a value based on where "
                      "clause. The value is either a constant or a jsonlogic "
                      "expression evaluated per row"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("series_name", "series name to create");
  clip.add_required<boost::json::value>(
    "value",
    "value to set: a constant (bool, int, float, string), or a jsonlogic "
    "expression given as a clippy expression or a raw jsonlogic object");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto where = clip.get<boost::json::object>("where");
  auto name_str = clip.get<std::string>("series_name");
  auto val = clip.get<boost::json::value>("value");

  metalldata::metall_graph::series_name name(name_str);

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  metalldata::result<> rc;
  if (val.is_object()) {
    // A clippy expression serializes as {"expression_type": ..., "rule": ...};
    // any other object is taken to be a raw jsonlogic rule.
    auto& obj = val.as_object();
    rc = mg.assign_jsonlogic(name, obj.contains("rule") ? obj["rule"] : val,
                             where_c);
  } else {
    auto sval =
      boost::json::value_to<metalldata::metall_graph::series_types>(val);
    rc = mg.assign_value(name, sval, where_c);
  }

  if (!rc) {
    comm.cerr0(rc.error());
    return -1;
  }

  for (const auto& [warn, count] : rc.warnings()) {
    comm.cerr0(std::format("{} : {}", warn, count));
  }

  clip.update_selectors(mg.get_selector_info());
  return 0;
} catch (const std::exception& e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
  return -1;
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
  return -1;
}
