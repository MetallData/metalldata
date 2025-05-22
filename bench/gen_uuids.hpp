#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

po::options_description gen_uuids_options() {
  po::options_description to_return(
      "gen_uuids:  creates a new parquet file and generates uuids");
  to_return.add_options()("metall_path", po::value<std::string>(),
                          "Path to Metall storage");
  return to_return;
}

int run_gen_uuids(ygm::comm& comm, const po::variables_map& vm) {
  if (!vm.count("metall_path")) {
    comm.cerr0("Error:  missing metall path for gen_uuids."); return -1;
  }
  std::string metall_path = vm["metall_path"].as<std::string>();

  if (std::filesystem::exists(metall_path)) {
    comm.cerr0(
        "Metall path already exists, it must be manually removed with 'rm' "
        "command");
    return -1;
  }
  comm.cf_barrier();

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

  for(size_t i=0; i<100;++i) {

    const auto record_id = record_store->add_record();
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::string uuid_string = boost::uuids::to_string(uuid);
    record_store->set<std::string_view>(series_index, record_id, uuid_string);
  }
  return 0;
}