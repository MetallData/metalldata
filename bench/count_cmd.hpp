#pragma once
#include "jsonlogic/jsonlogic.hpp"
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
namespace count_paths {
static const char* JL_PATH     = "jl_file";
static const char* METALL_PATH = "metall_path";

}  // namespace count_paths
class count_cmd : public base_subcommand {
 public:
  std::string name() override { return "count"; }
  std::string desc() override {
    return "Counts rows that match a JSONLogic expression.";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()(count_paths::METALL_PATH, po::value<std::string>(),
                     "Path to Metall storage");
    od.add_options()(count_paths::JL_PATH, po::value<std::string>(),
                     "Path to JSONLogic file (if not specified, use stdin)");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (!vm.contains(count_paths::METALL_PATH)) {
      return "Error: missing required options for subcommand";
    }

    metall_path = vm["metall_path"].as<std::string>();
    if (!std::filesystem::exists(metall_path)) {
      return std::string("Not found: ") + metall_path;
    }

    bjsn::value jl;

    if (!vm.contains(count_paths::JL_PATH)) {
      jl = jl::parseStream(std::cin);
    } else {
      auto jl_file = vm[count_paths::JL_PATH].as<std::string>();
      if (!std::filesystem::exists(jl_file)) {
        return std::string("Not found: ") + jl_file;
      }
      jl = jl::parseFile(jl_file);
    }
    bjsn::object& alljl = jl.as_object();
    jl_rule             = alljl["rule"];
    return {};
  }

  int run(ygm::comm& comm) override {
    comm.cout0("Count in: ", metall_path);
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;
    comm.cf_barrier();

    size_t count = 0;

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

    apply_jl(jl_rule, *record_store,
             [&count](record_store_type::record_id_type index) { count++; });

    comm.cout0(ygm::sum(count, comm), " entries passed JSONLogic filter.");

    return 0;
  }

 private:
  std::string metall_path;
  bjsn::value jl_rule;
};