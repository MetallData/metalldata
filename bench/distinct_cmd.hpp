#pragma once

// TODO: support where clauses for distinct.

#include "subcommand.hpp"
#include <iostream>

#include <ygm/comm.hpp>

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
    od.add_options()("parquet_path",
                     po::value<std::string>()->default_value(""),
                     "Path to Parquet");
    od.add_options()("series", po::value<std::string>(),
                     "series name to count unique");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    metall_path  = vm["metall_path"].as<std::string>();
    parquet_path = vm["parquet_path"].as<std::string>();
    if (vm.count("series")) {
      series = vm["series"].as<std::string>();
    } else {
      return "Error: missing required options for distinct";
    }
    return {};
  }

  int run(ygm::comm& comm) override {
    if (!metall_path.empty()) {
      if (!std::filesystem::exists(metall_path)) {
        comm.cerr0("Not found: ", metall_path);
        return 1;
      }
      comm.cf_barrier();

      metall::utility::metall_mpi_adaptor mpi_adaptor(
          metall::open_only, metall_path, comm.get_mpi_comm());
      auto& manager = mpi_adaptor.get_local_manager();

      static auto* record_store =
          manager.find<record_store_type>(metall::unique_instance).first;

      if (record_store->is_series_type<int64_t>(series)) {
        ygm::container::set<int64_t> distinct_ints(comm);
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
        comm.cout0("Number of unique items in series `", series, "' is ",
                   distinct_ints.size());
      } else if (record_store->is_series_type<std::string_view>(series)) {
        ygm::container::set<std::string> distinct_strings(comm);
        record_store->for_all_dynamic(
            series, [&comm, &distinct_strings](
                        const record_store_type::record_id_type index,
                        const auto                              value) {
              using T = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<T, std::string_view>) {
                distinct_strings.async_insert(std::string(value));
              } else {
                comm.cerr0("Unsupported  type");
                return 0;
              }
            });
        comm.cout0("Number of unique items in series `", series, "' is ",
                   distinct_strings.size());
      } else {
        comm.cout0("Only supported type is INT and STRING");
        return 1;
      }

    } else if (!parquet_path.empty()) {
      ygm::io::parquet_parser parquetp(comm, {parquet_path}, true);
      const auto&             schema = parquetp.get_schema();
      bool                    col_found{false};
      for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].name == series) {
          col_found = true;
        }
      }
      if (not col_found) {
        comm.cerr("Unkown column name: ", series);
        return 1;
      }
      ygm::container::set<std::string> distinct_strings(comm);
      parquetp.for_all({series}, [&comm, &distinct_strings](auto vfield) {
        std::visit(
            [&comm, &distinct_strings](auto field) {
              using T = std::decay_t<decltype(field)>;
              if constexpr (std::is_same_v<T, std::string>) {
                distinct_strings.async_insert(field);
              } else {
                comm.cerr0(
                    "Only strings supported right now for distinct of parquet "
                    "file");
                // return 0;
                exit(-1);
              }
            },
            vfield[0]);
      });
      comm.cout0("Number of unique items in series `", series, "' is ",
                 distinct_strings.size());

    } else {
      comm.cout0("Error: missing required options for distinct");
      return 1;
    }
    return 0;
  }

 private:
  std::string metall_path;
  std::string parquet_path;
  std::string series;
};
