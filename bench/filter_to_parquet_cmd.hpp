#pragma once
#include "jsonlogic/jsonlogic.hpp"
#include "metall/utility/metall_mpi_adaptor.hpp"
#include "mframe_bench.hpp"
#include "parquet_writer/parquet_writer.hpp"
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
namespace {
static const char* JL_PATH      = "jl_file";
static const char* METALL_PATH  = "metall_path";
static const char* PARQUET_PATH = "parquet_file";

}  // namespace
class filter_to_parquet_cmd : public base_subcommand {
 public:
  std::string name() override { return "filter_to_parquet"; }
  std::string desc() override {
    return "Given a metalldata path, a file containing a JSONLogic expression, "
           "the name of a parquet file to create, and a schema representing "
           "the columns of the metalldata, filter the metalldata and store the "
           "results in the parquet file.";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()(METALL_PATH, po::value<std::string>(),
                     "Path to Metall storage");
    od.add_options()(PARQUET_PATH, po::value<std::string>(),
                     "Name of parquet file to be created");

    od.add_options()(JL_PATH, po::value<std::string>(),
                     "Path to JSONLogic file (if not specified, use stdin)");

    od.add_options()("parquet_schema_str", po::value<std::string>(),
                     "Schema for parquet file (name:type,name:type)");

    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (!vm.contains(METALL_PATH) || !vm.contains(PARQUET_PATH) ||
        !vm.contains("parquet_schema_str")) {
      return "Error: missing required options for subcommand";
    }

    metall_path = vm[METALL_PATH].as<std::string>();
    if (!std::filesystem::exists(metall_path)) {
      return std::string("Not found: ") + metall_path;
    }

    parquet_path = vm[PARQUET_PATH].as<std::string>();
    if (std::filesystem::exists(parquet_path)) {
      return std::string("Parquet file ") + parquet_path + "already exists";
    }

    std::string parquet_schema = vm["parquet_schema"].as<std::string>();

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
    // STOPPED HERE 12 SEP
    pwriter = parquet_writer::ParquetWriter() return {};
  }

  int run(ygm::comm& comm) override {
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;
    comm.cf_barrier();

    // std::vector<size_t> records_to_erase;

    std::vector<bjsn::string> vars;
    jsonlogic::any_expr       expression_rule;

    pwriter = ParquetWriter() std::tie(expression_rule, vars, std::ignore) =
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
  std::string                   metall_path;
  std::string                   parquet_path;
  bjsn::value                   jl_rule;
  parquet_writer::ParquetWriter pwriter;
};