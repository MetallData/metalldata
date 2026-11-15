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

static result<metall_graph::where_clause> json2pred(const bjsn::value &v) {
  if (v.is_null()) {
    return metall_graph::where_clause{};
  }
  if (!v.is_object()) {
    return std::unexpected("template predicate must be an expression or null");
  }
  const auto &obj = v.get_object();
  if (obj.empty()) {
    return metall_graph::where_clause{};
  }
  if (!obj.contains("rule")) {
    return std::unexpected("template predicate invalid (norule)");
  }
  return metall_graph::where_clause(obj.at("rule"));
}

result<metall_graph::pattern_template> parse_pattern_template(
  const boost::json::object &obj) {
  metall_graph::pattern_template tmpl;
  if (!obj.contains("edges") || !obj.at("edges").is_array()) {
    return std::unexpected("template requires an \"edges\" array");
  }
  if (obj.contains("nodes") && !obj.at("nodes").is_object()) {
    return std::unexpected("template \"nodes\" must be an object");
  }

  // Vertices are numbered in order of first appearance: "nodes" keys, then
  // edge endpoints. Vertices without a "nodes" entry are wildcards.
  std::map<std::string, size_t, std::less<>> name_to_idx;
  auto vertex_idx = [&](std::string_view name) {
    auto [it, inserted] =
      name_to_idx.try_emplace(std::string(name), tmpl.vertex_names.size());
    if (inserted) {
      tmpl.vertex_names.emplace_back(name);
      tmpl.vertex_preds.emplace_back();
    }
    return it->second;
  };

  if (obj.contains("nodes")) {
    for (const auto &[name, pred_v] : obj.at("nodes").get_object()) {
      auto pred = json2pred(pred_v);
      if (!pred) {
        return std::unexpected(std::format(
          "template vertex {}: {}", std::string_view(name), pred.error()));
      }
      tmpl.vertex_preds[vertex_idx(name)] = std::move(pred.value());
    }
  }
  const size_t num_named = tmpl.vertex_names.size();
  std::vector<char> in_edge(num_named, 0);

  for (const auto &ev : obj.at("edges").get_array()) {
    if (!ev.is_array() || ev.get_array().size() < 2 ||
        ev.get_array().size() > 3) {
      return std::unexpected(
        "template edges must be [u, v] or [u, v, predicate]");
    }
    const auto &ea = ev.get_array();
    if (!ea[0].is_string() || !ea[1].is_string()) {
      return std::unexpected("template edge endpoints must be vertex names");
    }
    std::string_view u = ea[0].get_string();
    std::string_view v = ea[1].get_string();
    auto pred = ea.size() == 3 ? json2pred(ea[2])
                               : result<metall_graph::where_clause>{
                                   metall_graph::where_clause{}};
    if (!pred) {
      return std::unexpected(
        std::format("template edge {} - {}: {}", u, v, pred.error()));
    }
    for (auto idx : {vertex_idx(u), vertex_idx(v)}) {
      if (idx < num_named) {
        in_edge[idx] = 1;
      }
    }
    tmpl.edges.push_back({vertex_idx(u), vertex_idx(v), std::move(pred.value())});
  }

  for (size_t q = 0; q < num_named; ++q) {
    if (!in_edge[q]) {
      return std::unexpected(std::format("template vertex {} is not in any edge",
                                         tmpl.vertex_names[q]));
    }
  }

  if (auto v = tmpl.validate(); !v) {
    return std::unexpected(v.error());
  }
  return tmpl;
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