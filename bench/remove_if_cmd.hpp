#pragma once
#include "metall/utility/metall_mpi_adaptor.hpp"
#include "mframe_bench.hpp"
#include "subcommand.hpp"

#include <boost/json.hpp>
#include <boost/json/src.hpp>
#include <jsonlogic/logic.hpp>
#include <jsonlogic/src.hpp>
#include <ygm/comm.hpp>
#include <string>
#include <set>

#include <iostream>

namespace bjsn = boost::json;

inline bjsn::value parseStream(std::istream& inps) {
  bjsn::stream_parser p;
  std::string         line;

  // \todo skips ws in strings
  while (inps >> line) {
    std::error_code ec;

    p.write(line.c_str(), line.size(), ec);

    if (ec) return nullptr;
  }

  std::error_code ec;
  p.finish(ec);
  if (ec) return nullptr;

  return p.release();
}

inline bjsn::value parseFile(const std::string& filename) {
  std::ifstream is{filename};

  return parseStream(is);
}

static const char* JL_ARG     = "jl_file";
static const char* METALL_ARG = "metall_path";

class remove_if_cmd : public base_subcommand {
 public:
  std::string name() override { return "remove_if"; }
  std::string desc() override {
    return "Erases columns by provided JSONLogic expression.";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()(METALL_ARG, po::value<std::string>(),
                     "Path to Metall storage");
    od.add_options()(JL_ARG, po::value<std::string>(),
                     "Path to JSONLogic file (if not specified, use stdin)");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (!vm.contains(METALL_ARG)) {
      return "Error: missing required options for subcommand";
    }

    metall_path = vm["metall_path"].as<std::string>();
    if (!std::filesystem::exists(metall_path)) {
      return std::string("Not found: ") + metall_path;
    }

    bjsn::value jl;

    if (!vm.contains(JL_ARG)) {
      jl = parseStream(std::cin);
    } else {
      auto jl_file = vm["jl_file"].as<std::string>();
      if (!std::filesystem::exists(jl_file)) {
        return std::string("Not found: ") + jl_file;
      }
      jl = parseFile(jl_file);
    }
    bjsn::object& alljl = jl.as_object();
    jl_rule             = alljl["rule"];
    return {};
  }

  int run(ygm::comm& comm) override {
    comm.cout0("Remove if in: ", metall_path);
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;

    static std::set<std::string> keys_to_erase;
    comm.cf_barrier();

    static std::vector<size_t> records_to_erase;

    std::vector<bjsn::string> vars;
    jsonlogic::any_expr       expression_rule;

    std::tie(expression_rule, vars, std::ignore) =
        jsonlogic::create_logic(jl_rule);

    // jsonlogic::expr*                 rawexpr = expression_rule_.release();
    // std::shared_ptr<jsonlogic::expr> expression_rule{rawexpr};

    std::set<std::string> varset{};
    for (auto v : vars) {
      varset.emplace(v);
    }
    auto series = record_store->get_series_names();

    record_store->for_all_dynamic(

        [&comm, varset, series, &expression_rule](
            const record_store_type::record_id_type index,
            const auto                              series_values) {
          bjsn::object data{};
          bool         has_monostate = false;
          for (size_t i = 0; i < series.size(); ++i) {
            std::string s = series.at(i);
            auto        v = series_values.at(i);

            if (varset.contains(s)) {
              std::visit(
                  [&data, &s, &has_monostate](auto&& v) {
                    using T = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<T, std::monostate>) {
                      has_monostate = true;
                    } else {
                      data[s] = boost::json::value(v);
                    }
                  },
                  v);
              if (has_monostate) {
                break;
              }
            }
          }
          if (has_monostate) {
            return;
          }
          jsonlogic::any_expr res_j =
              jsonlogic::apply(expression_rule, jsonlogic::data_accessor(data));

          auto res = jsonlogic::unpack_value<bool>(res_j);
          if (res) {
            comm.cout0("Removing index ", index);
            records_to_erase.push_back(index);
          }
        });

    comm.cout0(ygm::sum(records_to_erase.size(), comm),
               " entries to be removed.");
    for (size_t index : records_to_erase) {
      record_store->remove_record(index);
    }
    keys_to_erase.clear();
    records_to_erase.clear();
    return 0;
  }

 private:
  std::string metall_path;
  bjsn::value jl_rule;
};