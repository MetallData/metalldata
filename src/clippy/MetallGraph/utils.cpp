#include <metalldata/metall_graph.hpp>
#include <boost/json.hpp>
#include <ygm/comm.hpp>
#include <expected>
#include <unordered_set>

namespace metalldata {

std::expected<metall_graph::series_name, metall_graph::return_code> obj2sn(
  const boost::json::object &obj) {
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

std::expected<std::unordered_set<metall_graph::series_name>,
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

std::expected<std::vector<metall_graph::series_name>, metall_graph::return_code>
obj2sn(const std::vector<boost::json::object> &objset) {
  metall_graph::return_code to_return;

  std::vector<metall_graph::series_name> sns;
  for (const auto &obj : objset) {
    auto r = obj2sn(obj);

    if (!r.has_value()) {
      return std::unexpected(r.error());
    }
    sns.push_back(metall_graph::series_name(r.value()));
  }

  return sns;
}

// enum class log_level {
//   off      = 0,
//   critical = 1,
//   error    = 2,
//   warn     = 3,
//   info     = 4,
//   debug    = 5
// };

ygm::log_level loglevel_py2ygm(int pyloglevel, ygm::log_level default_level) {
  switch (pyloglevel) {
    case 0:
      return ygm::log_level::off;
    case 10:
      return ygm::log_level::debug;
    case 20:
      return ygm::log_level::info;
    case 30:
      return ygm::log_level::warn;
    case 40:
      return ygm::log_level::error;
    case 50:
      return ygm::log_level::critical;
    default:
      return default_level;
  }
}

}  // namespace metalldata