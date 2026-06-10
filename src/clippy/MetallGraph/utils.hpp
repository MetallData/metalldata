#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
result<metall_graph::series_name> obj2sn(const boost::json::object &obj);

result<std::unordered_set<metall_graph::series_name>> obj2sn(
  const std::unordered_set<boost::json::object> &objset);

result<std::vector<metall_graph::series_name>> obj2sn(
  const std::vector<boost::json::object> &objset);

ygm::log_level loglevel_py2ygm(
  int pyloglevel, ygm::log_level default_level = ygm::log_level::warn);

}  // namespace metalldata