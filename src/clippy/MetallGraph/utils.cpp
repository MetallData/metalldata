// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <metalldata/metall_graph.hpp>
#include <boost/json.hpp>
#include <ygm/comm.hpp>
#include <expected>
#include <unordered_set>

namespace metalldata {

result<metall_graph::series_name> obj2sn(const boost::json::object &obj) {
  if (!obj.contains("rule")) {
    return std::unexpected("series name invalid (norule)");
  }

  auto rule = obj.at("rule");
  auto rule_obj = rule.get_object();
  if (!rule_obj.contains("var")) {
    return std::unexpected("series name invalid (novar)");
  }
  return metall_graph::series_name(rule_obj.at("var").as_string());
}

// result<std::unordered_set<metall_graph::series_name>> obj2sn(
//   const std::unordered_set<boost::json::object> &objset) {
//   std::unordered_set<metall_graph::series_name> sns;
//   for (const auto &obj : objset) {
//     auto r = obj2sn(obj);

//     if (!r.has_value()) {
//       return std::unexpected(r.error());
//     }
//     sns.insert(metall_graph::series_name(r.value()));
//   }

//   return sns;
// }

result<std::vector<metall_graph::series_name>> obj2sn(
  const std::vector<boost::json::object> &objs) {
  std::vector<metall_graph::series_name> sns;
  for (const auto &obj : objs) {
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

// given a vector of "row" data and a corresponding vector of series names,
// return a json array suitable for passing back to clippy.
bjsn::array rows_to_json(
  std::vector<std::vector<metalldata::metall_graph::data_types>> rows,
  std::vector<metall_graph::series_name>                         series_names) {
  bjsn::array json_rows{};
  json_rows.reserve(rows.size());

  for (const auto &row : rows) {
    bjsn::object rowmap;
    for (size_t i = 0; i < row.size(); ++i) {
      auto sname = series_names.at(i);
      auto sval = row[i];
      std::visit(
        [&](const auto &val) {
          if constexpr (!std::is_same_v<std::decay_t<decltype(val)>,
                                        std::monostate>) {
            rowmap[sname.qualified()] = val;
          }
        },
        sval);
    }
    json_rows.emplace_back(rowmap);
  }

  return json_rows;
}

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