// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1

#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>
#include <clippy/clippy.hpp>
#include <string>
#include <unordered_set>
#include <boost/json.hpp>
#include <ygm/utility/boost_json.hpp>
#include "utils.hpp"

static const std::string method_name    = "topk";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Returns the top k nodes or edges."};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<boost::json::object>("series", "The series to compare");
  clip.add_optional<size_t>("k", "the number of nodes/edges to return", 10);
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});
  clip.add_optional<std::vector<boost::json::object>>(
    "addl_series",
    "Additional series names to include. Series must be the same type as the "
    "`series` parameter.",
    {});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path   = clip.get_state<std::string>("path");
  auto where  = clip.get<boost::json::object>("where");
  auto sn_obj = clip.get<boost::json::object>("series");

  auto try_sn = metalldata::obj2sn(sn_obj);
  if (!try_sn.has_value()) {
    comm.cerr0(try_sn.error().error);
    return -1;
  }

  auto                                   comp_series = try_sn.value();
  auto                                   k           = clip.get<size_t>("k");
  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  auto addl_series_obj_vec =
    clip.get<std::vector<boost::json::object>>("addl_series");
  auto try_obj = metalldata::obj2sn(addl_series_obj_vec);
  if (!try_obj.has_value()) {
    comm.cerr0(try_obj.error().error);
    return -1;
  }

  auto addl_series_vec = try_obj.value();
  for (const auto& sn : addl_series_vec) {
    if (sn.prefix() != comp_series.prefix()) {
      comm.cerr0("additional series names must be ", comp_series.prefix(),
                 " series");
      return -1;
    }
  }

  std::vector<metalldata::metall_graph::series_name> addl_series{
    addl_series_vec.begin(), addl_series_vec.end()};

  auto topk =
    mg.topk(k, comp_series, addl_series, std::greater<void>(), where_c);

  boost::json::array json_rows;

  for (const auto& row : topk) {
    boost::json::array json_row;
    for (const auto& var_el : row) {
      std::visit(
        [&json_row](auto el) {
          if constexpr (std::is_same_v<decltype(el), std::monostate>) {
            json_row.push_back(boost::json::value(nullptr));
          } else {
            boost::json::value j_el(el);
            json_row.push_back(j_el);
          }
        },
        var_el);
    }
    json_rows.push_back(json_row);
  }
  clip.to_return(json_rows);
  return 0;
}