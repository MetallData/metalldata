#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {
std::expected<metall_graph::series_name, metall_graph::return_code> obj2sn(
  const boost::json::object &obj);

std::expected<std::unordered_set<metall_graph::series_name>,
              metall_graph::return_code>
obj2sn(const std::unordered_set<boost::json::object> &objset);

std::expected<std::vector<metall_graph::series_name>, metall_graph::return_code>
obj2sn(const std::vector<boost::json::object> &objset);

ygm::log_level loglevel_py2ygm(
  int pyloglevel, ygm::log_level default_level = ygm::log_level::warn);

}  // namespace metalldata