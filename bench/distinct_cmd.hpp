#pragma once

#include "mframe_bench.hpp"
#include "subcommand.hpp"
#include "jsonlogic/jsonlogic.hpp"
#include <boost/json.hpp>
#include <boost/json/src.hpp>
#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>

namespace bjsn = boost::json;

class distinct_cmd : public base_subcommand {
 public:
  std::string name() override { return "distinct"; }
  std::string desc() override {
    return "Calculates the number of unqiue datapoints in a column.";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()("metall_path", po::value<std::string>()->default_value(""),
                     "Path to Metall storage");
    od.add_options()("series", po::value<std::string>(),
                     "series name to count unique");
    od.add_options()("where_file", po::value<std::string>(),
                     "File containing jsonlogic filter for 'where'");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    metall_path = vm["metall_path"].as<std::string>();
    if (vm.count("series")) {
      series = vm["series"].as<std::string>();
    } else {
      return "Error: missing required options for distinct";
    }

    bjsn::value jl;

    if (!vm.contains("metall_path")) {
      return "Error: metall_path required";
    }
    if (!vm.contains("where_file")) {
      jl_rule = std::nullopt;
    } else {
      auto jl_file = vm["where_file"].as<std::string>();
      if (!std::filesystem::exists(jl_file)) {
        return std::string("Not found: ") + jl_file;
      }
      jl                  = jl::parseFile(jl_file);
      bjsn::object& alljl = jl.as_object();
      jl_rule             = alljl["rule"];
    }
    return {};
  }

  int run(ygm::comm& comm) override {
    if (!std::filesystem::exists(metall_path)) {
      comm.cerr0("Metall path not found: ", metall_path);
      return 1;
    }
    comm.cf_barrier();

    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;

    if (record_store->is_series_type<int64_t>(series)) {
      ygm::container::counting_set<int64_t> distinct_ints(comm);
      if (jl_rule) {
        apply_jl_series(
            series, jl_rule.value(), *record_store,
            [&comm, &distinct_ints](record_store_type::record_id_type inde,
                                    const auto                        value) {
              using T = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<T, int64_t>) {
                distinct_ints.async_insert(int64_t(value));
              } else {
                comm.cerr0("Unsupported type");
                return 0;
              }
            });
      } else {
        record_store->for_all_dynamic(
            series, [&comm, &distinct_ints](
                        const record_store_type::record_id_type index,
                        const auto                              value) {
              using T = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<T, int64_t>) {
                distinct_ints.async_insert(int64_t(value));
              } else {
                comm.cerr0("Unsupported type");
                return 0;
              }
            });
      }
      comm.cout0("Number of unique items in series `", series, "' is ",
                 distinct_ints.size());
      return 0;

    } else if (record_store->is_series_type<uint64_t>(series)) {
      ygm::container::counting_set<uint64_t> distinct_uints(comm);
      std::string_view                       series_name = series;
      if (jl_rule) {
        apply_jl_series(
            series, jl_rule.value(), *record_store,
            [&comm, &distinct_uints, &series_name](
                record_store_type::record_id_type index, const auto value) {
              if (!std::holds_alternative<uint64_t>(value)) {
                comm.cerr0("Unsupported type (want uint64_t)");
                return;
              }

              auto v = std::get<uint64_t>(value);

              distinct_uints.async_insert(v);
            });

      } else {
        record_store->for_all_dynamic(
            series, [&comm, &distinct_uints](
                        const record_store_type::record_id_type index,
                        const auto                              value) {
              using T = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<T, uint64_t>) {
                distinct_uints.async_insert(uint64_t(value));
              } else {
                comm.cerr0("Unsupported type");
                return;
              }
            });
      }
      comm.cout0("Number of unique items in series `", series, "' is ",
                 distinct_uints.size());
      return 0;

    } else if (record_store->is_series_type<std::string_view>(series)) {
      ygm::container::counting_set<std::string> distinct_strings(comm);
      if (jl_rule) {
        apply_jl_series(
            series, jl_rule.value(), *record_store,
            [&comm, &distinct_strings](record_store_type::record_id_type index,
                                       const auto value) {
              using T = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<T, std::string_view>) {
                distinct_strings.async_insert(std::string(value));
              } else {
                comm.cerr0("Unsupported type");
                return;
              }
            });
      } else {
        record_store->for_all_dynamic(
            series, [&comm, &distinct_strings](
                        const record_store_type::record_id_type index,
                        const auto                              value) {
              using T = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<T, std::string_view>) {
                distinct_strings.async_insert(std::string(value));
              } else {
                comm.cerr0("Unsupported  type");
                return;
              }
            });
      }
      comm.cout0("Number of unique items in series `", series, "' is ",
                 distinct_strings.size());
      return 0;
    }
    comm.cout0("Only supported types are int, uint, and string");
    return 1;
  }

 private:
  std::string                metall_path;
  std::string                parquet_path;
  std::optional<bjsn::value> jl_rule;
  std::string                series;
};
