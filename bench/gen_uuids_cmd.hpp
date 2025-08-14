#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "subcommand.hpp"
#include <iostream>

#include <ygm/comm.hpp>

class gen_uuids_cmd : public base_subcommand {
 public:
  std::string name() override { return "gen_uuids"; }
  std::string desc() override {
    return "Creates a new metall with a column of uuids.";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()("metall_path", po::value<std::string>(),
                     "Path to Metall storage");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (!vm.count("metall_path")) {
      return "Error:  missing metall path for gen_uuids.";
    }
    std::string metall_path = vm["metall_path"].as<std::string>();

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

    auto series_index = record_store->add_series<std::string_view>("uuids");

    for (size_t i = 0; i < 100; ++i) {
      const auto         record_id   = record_store->add_record();
      boost::uuids::uuid uuid        = boost::uuids::random_generator()();
      std::string        uuid_string = boost::uuids::to_string(uuid);
      record_store->set<std::string_view>(series_index, record_id, uuid_string);
    }
    return 0;
  }

 private:
  std::string metall_path;
};
