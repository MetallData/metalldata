#pragma once

#include "metall/utility/metall_mpi_adaptor.hpp"
#include "mframe_bench.hpp"
#include "subcommand.hpp"

#include <ygm/comm.hpp>

<template typename T, Fn> T gen_faker_data(Fn fn) {}

class add_series_cmd : public base_subcommand {
 public:
  std::string name() override { return "add_series"; }
  std::string desc() override {
    return "Creates a new column in a metall dataframe with a column of faker "
           "data of selected types.";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()("metall_path", po::value<std::string>(),
                     "Path to Metall storage");
    od.add_options()("series_type", po::value<std::string>(),
                     "Type of data to add");
    od.add_options()("series_name", po::value<std::string>(), "Name of series");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (!vm.contains("metall_path")) {
      return "Error:  missing metall path for add_series.";
    }
    if (!vm.contains("series_type")) {
      return "Error:  missing series type for add_series.";
    }
    if (!vm.contains("series_name")) {
      return "Error:  missing series name for add_series.";
    }

    std::string metall_path = vm["metall_path"].as<std::string>();
    std::string series_name = vm["series_name"].as<std::string>();

    if (std::filesystem::exists(metall_path)) {
      return "Metall path already exists, it must be manually removed with "
             "'rm' command";
    }
    return {};
  }

  int run(ygm::comm& comm) override {
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::create_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    auto* string_store = manager.construct<string_store_type>(
        metall::unique_instance)(manager.get_allocator());
    static auto* record_store = manager.construct<record_store_type>(
        metall::unique_instance)(string_store, manager.get_allocator());

    auto* pm_hash_key = manager.construct<persistent_string>("hash_key")(
        "NONE", manager.get_allocator());

    auto series_index = record_store->add_series<std::string_view>(series_name);

    for (record_id : record_store->for_all<typename series_type>(
             const std::string_view series_name, series_func_t series_func))
      for (size_t i = 0; i < 100; ++i) {
        const auto         record_id   = record_store->add_record();
        boost::uuids::uuid uuid        = boost::uuids::random_generator()();
        std::string        uuid_string = boost::uuids::to_string(uuid);
        record_store->set<std::string_view>(series_index, record_id,
                                            uuid_string);
      }
    return 0;
  }

 private:
  std::string metall_path;
  std::string series_name;
};
