// Copyright 2025 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <faker-cxx/number.h>
#include <faker-cxx/string.h>
#include <faker-cxx/date.h>
#include <faker-cxx/internet.h>
#include <faker-cxx/person.h>
#include <boost/program_options.hpp>
#include <iostream>

#include <unordered_map>
#include <functional>

#include <multiseries/multiseries_record.hpp>
#include <string_table/string_store.hpp>
#include <ygm/comm.hpp>
#include <ygm/utility/timer.hpp>
#include <ygm/utility/progress_indicator.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>

#include "subcommand.hpp"

namespace po = boost::program_options;
using namespace multiseries;

// Type-erased generator function
namespace {

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

using persistent_string =
    boost::container::basic_string<char, std::char_traits<char>,
                                   metall::manager::allocator_type<char>>;

using generator_func = std::function<void(record_store_type&, size_t,
                                          record_store::record_id_type)>;

}  // namespace

// Generator registry
class GeneratorRegistry {
 public:
  void register_generator(const std::string& type_name, generator_func gen) {
    generators_[type_name] = gen;
  }

  generator_func get_generator(const std::string& type_name) const {
    auto it = generators_.find(type_name);
    return (it != generators_.end()) ? it->second : nullptr;
  }

  std::vector<std::string> get_available_types() const {
    std::vector<std::string> types;
    for (const auto& [name, _] : generators_) {
      types.push_back(name);
    }
    return types;
  }

 private:
  std::unordered_map<std::string, generator_func> generators_;
};

// Initialize all generators
inline GeneratorRegistry create_registry() {
  GeneratorRegistry registry;

  registry.register_generator(
      "uuid4", [](record_store_type& store, size_t series_idx,
                  record_store::record_id_type record_id) {
        auto uuid = faker::string::uuidV4();
        store.set<std::string_view>(series_idx, record_id,
                                    std::string_view(uuid));
      });

  registry.register_generator(
      "integer", [](record_store_type& store, size_t series_idx,
                    record_store::record_id_type record_id) {
        store.set<int64_t>(series_idx, record_id,
                           faker::number::integer<int64_t>(10'000'000));
      });

  registry.register_generator(
      "uint", [](record_store_type& store, size_t series_idx,
                 record_store::record_id_type record_id) {
        store.set<uint64_t>(series_idx, record_id,
                            faker::number::integer<uint64_t>(10'000'000));
      });

  registry.register_generator(
      "double", [](record_store_type& store, size_t series_idx,
                   record_store::record_id_type record_id) {
        store.set<double>(series_idx, record_id,
                          faker::number::decimal<double>(10'000'000.0));
      });

  registry.register_generator(
      "percentage", [](record_store_type& store, size_t series_idx,
                       record_store::record_id_type record_id) {
        store.set<double>(series_idx, record_id,
                          faker::number::decimal<double>(0.0, 100.0));
      });

  registry.register_generator(
      "int_percentage", [](record_store_type& store, size_t series_idx,
                           record_store::record_id_type record_id) {
        store.set<uint64_t>(series_idx, record_id,
                            faker::number::integer<uint64_t>(100));
      });

  registry.register_generator(
      "two_char_string",
      [](record_store_type& store, size_t series_idx,
         record_store::record_id_type record_id) -> void {
        store.set<std::string_view>(series_idx, record_id,
                                    faker::string::alpha(2));
      });
  registry.register_generator(
      "bool", [](record_store_type& store, size_t series_idx,
                 record_store::record_id_type record_id) {
        store.set<bool>(series_idx, record_id,
                        faker::number::integer(0, 1) == 1);
      });

  registry.register_generator(
      "name", [](record_store_type& store, size_t series_idx,
                 record_store::record_id_type record_id) {
        auto name = faker::person::fullName();
        store.set<std::string_view>(series_idx, record_id,
                                    std::string_view(name));
      });

  registry.register_generator(
      "email", [](record_store_type& store, size_t series_idx,
                  record_store::record_id_type record_id) {
        auto email = faker::internet::email();
        store.set<std::string_view>(series_idx, record_id,
                                    std::string_view(email));
      });

  registry.register_generator(
      "username", [](record_store_type& store, size_t series_idx,
                     record_store::record_id_type record_id) {
        auto username = faker::internet::username();
        store.set<std::string_view>(series_idx, record_id,
                                    std::string_view(username));
      });

  registry.register_generator(
      "timestamp", [](record_store_type& store, size_t series_idx,
                      record_store::record_id_type record_id) {
        store.set<int64_t>(
            series_idx, record_id,
            faker::number::integer<int64_t>(1640995200, 1735689600));
      });

  return registry;
}

// Series configuration
struct SeriesConfig {
  std::string name;
  std::string type;

  size_t add_to_store(record_store_type& store) const {
    if (type == "uuid4" || type == "name" || type == "email" ||
        type == "username" || type == "two_char_string") {
      return store.add_series<std::string_view>(name);
    } else if (type == "integer" || type == "timestamp") {
      return store.add_series<int64_t>(name);
    } else if (type == "uint" || type == "int_percentage") {
      return store.add_series<uint64_t>(name);
    } else if (type == "double" || type == "percentage") {
      return store.add_series<double>(name);
    } else if (type == "bool") {
      return store.add_series<bool>(name);
    }
    throw std::runtime_error("Unknown type: " + type);
  }
};

// Parse series from command line format "name:type"
inline std::vector<SeriesConfig> parse_series(
    const std::vector<std::string>& series_args) {
  std::vector<SeriesConfig> configs;

  for (const auto& arg : series_args) {
    size_t colon_pos = arg.find(':');
    if (colon_pos == std::string::npos) {
      throw std::runtime_error("Invalid series format: " + arg +
                               " (expected name:type)");
    }

    std::string name = arg.substr(0, colon_pos);
    std::string type = arg.substr(colon_pos + 1);

    if (name.empty() || type.empty()) {
      throw std::runtime_error("Invalid series format: " + arg +
                               " (name or type is empty)");
    }

    configs.push_back({name, type});
  }

  return configs;
}

class gen_faker_cmd : public base_subcommand {
 public:
  std::string name() override { return "gen-multiseries"; }

  std::string desc() override {
    return "Generate synthetic multiseries data using faker library";
  }

  po::options_description get_options() override {
    po::options_description desc("Generate Multiseries Data Options");
    desc.add_options()("metall_path, m", po::value<std::string>(),
                       "Metall datastore path")(
        "n_rows,n", po::value<int64_t>()->default_value(1'000'000),
        "Total number of rows to generate (default 1000000)")(
        "series,s", po::value<std::vector<std::string>>()->multitoken(),
        "Series specifications in format name:type (e.g., user_id:uuid4)")(
        "list-types", "List available data types");
    return desc;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    registry = create_registry();

    if (vm.contains("list-types")) {
      ygm::wcout0() << "Available data types:\n";
      for (const auto& type : registry.get_available_types()) {
        ygm::wcout0() << "  " << type << std::endl;
      }
      exit(0);
    }

    if (!vm.contains("metall_path")) {
      return "Error: missing required options for subcommand";
    }

    metall_path = vm["metall_path"].as<std::string>();
    if (std::filesystem::exists(metall_path)) {
      return metall_path + " already exists; aborting";
    }

    n_rows         = vm["n_rows"].as<int64_t>();
    series_configs = parse_series(vm["series"].as<std::vector<std::string>>());

    // Validate series types
    for (const auto& config : series_configs) {
      if (!registry.get_generator(config.type)) {
        throw std::runtime_error("Unknown data type: " + config.type);
      }
    }

    return {};
  }

  int run(ygm::comm& comm) override {
    // Calculate work distribution
    const int64_t rows_per_rank = n_rows / comm.size();
    const int64_t start_row     = comm.rank() * rows_per_rank;
    const int64_t end_row =
        (comm.rank() == comm.size() - 1) ? n_rows : start_row + rows_per_rank;

    if (comm.rank() == 0) {
      std::cout << "Generating " << n_rows << " rows across " << comm.size()
                << " ranks\n";
      std::cout << "Series configuration:\n";
      for (const auto& config : series_configs) {
        std::cout << "  " << config.name << " : " << config.type << std::endl;
      }
      std::cout << "Datastore: " << metall_path << std::endl;
    }
    ygm::utility::progress_indicator pi(
        comm, {.update_freq = 10000, .message = "Records generated"});

    ygm::utility::timer timer{};

    // Create/open Metall datastore
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::create_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    auto* string_store = manager.construct<string_store_type>(
        metall::unique_instance)(manager.get_allocator());
    static auto& record_store = *manager.construct<record_store_type>(
        metall::unique_instance)(string_store, manager.get_allocator());

    // Add series and collect generators (only on rank 0, then broadcast
    // structure)
    std::vector<std::pair<size_t, generator_func>> series_generators;

    for (const auto& config : series_configs) {
      auto series_idx = config.add_to_store(record_store);
      auto generator  = registry.get_generator(config.type);
      series_generators.emplace_back(series_idx, generator);
    }

    comm.barrier();

    // Generate data
    for (int64_t row_id = start_row; row_id < end_row; ++row_id) {
      auto record_id = record_store.add_record();

      // Generate data for each series
      for (const auto& [series_idx, generator] : series_generators) {
        generator(record_store, series_idx, record_id);
      }

      pi.async_inc();
    }
    pi.complete();
    comm.barrier();

    size_t local_ttl = record_store.num_records();

    size_t ttl_recs = ygm::sum(local_ttl, comm);
    comm.barrier();

    comm.cout0() << "\nGeneration completed in " << timer.elapsed()
                 << " seconds\n";
    comm.cout0() << "Total records: " << ttl_recs << std::endl;
    comm.cout0() << "Total series: " << record_store.num_series() << std::endl;

    return 0;
  }

 private:
  std::string               metall_path;
  std::vector<SeriesConfig> series_configs;
  int                       n_rows;
  GeneratorRegistry         registry;
};