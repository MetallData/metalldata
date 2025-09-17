#pragma once
#include "jsonlogic/jsonlogic.hpp"
#include "metall/utility/metall_mpi_adaptor.hpp"
#include "mframe_bench.hpp"
#include "parquet_writer/parquet_writer.hpp"
#include "subcommand.hpp"
#include "ygm/utility/world.hpp"

#include <boost/json.hpp>
#include <boost/json/src.hpp>
#include <jsonlogic/logic.hpp>
#include <jsonlogic/src.hpp>
#include <ygm/comm.hpp>
#include <string>
#include <unordered_map>
#include <format>
#include <iostream>
#include <optional>

namespace bjsn = boost::json;
namespace f2p {
static const char* METALL_PATH  = "metall_path";
static const char* PARQUET_PATH = "parquet_file";
static const char* JL_PATH      = "jl_file";

template <typename T>
constexpr std::optional<char> get_type_char() {
  if constexpr (std::is_same_v<T, int64_t>)
    return 'i';
  else if constexpr (std::is_same_v<T, uint64_t>)
    return 'u';
  else if constexpr (std::is_same_v<T, std::string_view>)
    return 's';
  else if constexpr (std::is_same_v<T, double>)
    return 'f';
  else if constexpr (std::is_same_v<T, bool>)
    return 'b';
  else
    return std::nullopt;  // this includes monostate
}

}  // namespace f2p
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
    od.add_options()(f2p::METALL_PATH, po::value<std::string>(),
                     "Path to Metall storage");
    od.add_options()(f2p::PARQUET_PATH, po::value<std::string>(),
                     "Name of parquet file to be created");

    od.add_options()(f2p::JL_PATH, po::value<std::string>(),
                     "Path to JSONLogic file (if not specified, use stdin)");

    // od.add_options()("schema", po::value<std::string>(),
    //                  "Schema for parquet file (name:type,name:type)");
    // od.add_options()("delimiter", po::value<char>()->default_value(':'),
    //                  "Delimiter for type information");

    od.add_options()("batch_size",
                     po::value<size_t>()->default_value(1'000'000),
                     "Parquet batch size");

    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (!vm.contains(f2p::METALL_PATH) || !vm.contains(f2p::PARQUET_PATH)) {
      return "Error: missing required options for subcommand";
    }

    metall_path = vm[f2p::METALL_PATH].as<std::string>();
    if (!std::filesystem::exists(metall_path)) {
      return std::format("Not found: {}", metall_path);
    }

    parquet_path = std::format(
        "{}_{}.parquet", vm[f2p::PARQUET_PATH].as<std::string>(), ygm::wrank());
    if (std::filesystem::exists(parquet_path)) {
      return std::format("Parquet file {} already exists", parquet_path);
    }

    batch_size = vm["batch_size"].as<size_t>();

    bjsn::value jl;

    if (!vm.contains(f2p::JL_PATH)) {
      jl = jl::parseStream(std::cin);
    } else {
      auto jl_file = vm[f2p::JL_PATH].as<std::string>();
      if (!std::filesystem::exists(jl_file)) {
        return std::format("Not found: {}", jl_file);
      }
      jl = jl::parseFile(jl_file);
    }
    bjsn::object& alljl = jl.as_object();
    jl_rule             = alljl["rule"];

    return {};
  }

  int run(ygm::comm& comm) override {
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;
    if (!pwriter) {  // lazily compute the schema.
      std::vector<std::string> series_names = record_store->get_series_names();
      std::vector<char>        series_types;
      series_types.reserve(series_names.size());

      if (record_store->num_records() <= 0) {  // no records!
        return 0;  // TODO: this should probably be an error
      }

      std::unordered_map<std::string, char> name_to_type{};
      record_store->for_all_dynamic(
          // for each row, we check each series value. If we can
          // determine its type (that is, it's not monostate/null), then
          // we add it to the name_to_type map. Once we have all the series
          // types, we exit early. If we exit without getting all the types,
          // we exit. (TODO: This should be an error, probably.)
          [&name_to_type, &series_names](size_t index, auto) {
            for (const auto& name : series_names) {
              // try to get the type of the series value.
              record_store->visit_field(
                  name, index,
                  [&name_to_type, &series_names, &name](const auto& value) {
                    using T = std::decay_t<decltype(value)>;
                    if (auto series_type = f2p::get_type_char<T>()) {
                      // series_type is not nullopt - we have a type.
                      name_to_type[name] = *series_type;
                      if (name_to_type.size() == series_names.size()) {
                        // we've got all the types. Let's exit early.
                        return;
                      }
                    }
                  });
            }
          });
      if (name_to_type.size() != series_names.size()) {
        comm.cerr0("Missing types; aborting");
        return 0;
      }
      std::vector<std::string> parquet_schema;
      for (const auto& name : series_names) {
        parquet_schema.push_back(
            std::format("{}:{}", name, name_to_type[name]));
      }

      //////////////////////////////////////////////////////////
      // This is just for diagnostics; we can get rid of it.
      std::string parquet_schema_str;
      for (size_t i = 0; i < parquet_schema.size(); ++i) {
        if (i > 0) parquet_schema_str += ", ";
        parquet_schema_str += parquet_schema[i];
      }
      comm.cout0("parquet_schema: ", parquet_schema_str);
      //////////////////////////////////////////////////////////

      pwriter.emplace(parquet_path, parquet_schema, ':', batch_size);
    }

    comm.cf_barrier();

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

    auto   pwr = &pwriter.value();
    size_t i   = 0;
    // TODO: what should we do if there's an arrow error in write_row?
    apply_jl(jl_rule, *record_store,
             [pwr, &i, &comm](record_store_type::record_id_type index,
                              const auto&                       series_values) {
               auto st = pwr->write_row(series_values);
               if (!st.ok()) {
                 comm.cerr0("status is bad: ", st);
               }
               i++;
             });

    comm.cout0(i, " entries written.");
    return 0;
  }

 private:
  std::string                   metall_path;
  std::string                   parquet_path;
  bjsn::value                   jl_rule;
  std::optional<parquet_writer::ParquetWriter> pwriter;
  size_t                                       batch_size;
};