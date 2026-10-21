// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
result<metall_graph::series_name> obj2sn(const boost::json::object &obj);

result<std::unordered_set<metall_graph::series_name>> obj2sn(
  const std::unordered_set<boost::json::object> &objset);

result<std::vector<metall_graph::series_name>> obj2sn(
  const std::vector<boost::json::object> &objset);

bjsn::array rows_to_json(
  std::vector<std::vector<metalldata::metall_graph::data_types>> rows,
  std::vector<metall_graph::series_name>                         series_names);

/// Parses a pattern template of the form
///   {"edges": [["a", "b"], ["b", "c", <pred|null>], ...],
///    "nodes": {"a": <pred|null>, ...}}           // optional
/// Template vertices are the edge endpoints; "nodes" only attaches predicates
/// to some of them. <pred> is a serialized jsonlogic expression
/// ({"rule": ...}). null, a missing predicate, or {} matches everything.
result<metall_graph::pattern_template> parse_pattern_template(
  const boost::json::object &obj);

ygm::log_level loglevel_py2ygm(
  int pyloglevel, ygm::log_level default_level = ygm::log_level::warn);

}  // namespace metalldata