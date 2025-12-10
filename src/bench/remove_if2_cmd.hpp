#pragma once
#include "metall_jl/metall_jl.hpp"
#include "metall/utility/metall_mpi_adaptor.hpp"
#include "mframe_bench.hpp"
#include "subcommand.hpp"

#include <ygm/comm.hpp>
#include <string>
#include <set>

#include <iostream>

namespace bjsn = boost::json;
namespace {
static const char* JL_PATH     = "jl_file";
static const char* METALL_PATH = "metall_path";

}  // namespace
class remove_if2_cmd : public base_subcommand {
 public:
  std::string name() override { return "remove_if2"; }
  std::string desc() override {
    return "Erases columns by provided JSONLogic expression.";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()(METALL_PATH, po::value<std::string>(),
                     "Path to Metall storage");
    od.add_options()(JL_PATH, po::value<std::string>(),
                     "Path to JSONLogic file (if not specified, use stdin)");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (!vm.contains(METALL_PATH)) {
      return "Error: missing required options for subcommand";
    }

    metall_path = vm["metall_path"].as<std::string>();
    if (!std::filesystem::exists(metall_path)) {
      return std::string("Not found: ") + metall_path;
    }

    bjsn::value jl;

    if (!vm.contains(JL_PATH)) {
      jl = jl::parseStream(std::cin);
    } else {
      auto jl_file = vm["jl_file"].as<std::string>();
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
    comm.cout0("Remove if in: ", metall_path);
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;
    comm.cf_barrier();

    std::vector<size_t> records_to_erase;

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

    apply_jl(
        jl_rule, *record_store,
        [&records_to_erase](record_store_type::record_id_type index,
                            const auto) { records_to_erase.push_back(index); });

    comm.cout0(ygm::sum(records_to_erase.size(), comm),
               " entries to be removed.");
    for (size_t index : records_to_erase) {
      record_store->remove_record(index);
    }

    records_to_erase.clear();
    return 0;
  }

 private:
  std::string metall_path;
  bjsn::value jl_rule;
};