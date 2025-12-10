#pragma once
#include <metall_graph.hpp>
#include <boost/json.hpp>
#include <expected>

namespace metalldata {

inline std::expected<metall_graph::series_name, metall_graph::return_code>
obj2sn(const boost::json::object &obj) {
  metall_graph::return_code to_return;
  if (!obj.contains("rule")) {
    to_return.error = "Series name invalid (norule)";
    return std::unexpected(to_return);
  }

  auto rule     = obj.at("rule");
  auto rule_obj = rule.get_object();
  if (!rule_obj.contains("var")) {
    to_return.error = "Series name invalid (novar)";
    return std::unexpected(to_return);
  }
  return metall_graph::series_name(rule_obj.at("var").as_string());
}

inline std::expected<std::unordered_set<metall_graph::series_name>,
                     metall_graph::return_code>
obj2sn(const std::unordered_set<boost::json::object> &objset) {
  metall_graph::return_code to_return;

  std::unordered_set<metall_graph::series_name> sns;
  for (const auto &obj : objset) {
    auto r = obj2sn(obj);

    if (!r.has_value()) {
      return std::unexpected(r.error());
    }
    sns.insert(metall_graph::series_name(r.value()));
  }

  return sns;
}
}  // namespace metalldata