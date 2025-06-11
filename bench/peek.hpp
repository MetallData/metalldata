#pragma once

po::options_description peek_options() {
  po::options_description to_return(
      "peek:  peeks at metall or parquet data (choose one)");
  to_return.add_options()("metall_path",
                          po::value<std::string>()->default_value(""),
                          "Path to Metall storage");
  to_return.add_options()("parquet_path",
                          po::value<std::string>()->default_value(""),
                          "Path to Parquet");
  return to_return;
}

int run_peek(ygm::comm& comm, const po::variables_map& vm) {
  std::string metall_path  = vm["metall_path"].as<std::string>();
  std::string parquet_path = vm["parquet_path"].as<std::string>();
  if (!metall_path.empty()) {
    if (!std::filesystem::exists(metall_path)) {
      comm.cerr0("Not found: ", metall_path);
      return 1;
    }
    comm.cf_barrier();

    comm.cout0("Peek at: ", metall_path);
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;

    auto* pm_hash_key = manager.find<persistent_string>("hash_key").first;

    comm.cout0("Series Count: ", record_store->num_series());
    // todo:  manually count
    comm.cout0("Record Count: ", ygm::sum(record_store->num_records(), comm));
    comm.cout0("Hash key = ", *pm_hash_key);

    std::vector<std::string> series_names = record_store->get_series_names();
    for (const auto& name : series_names) {
      comm.cout0() << name << "\t\t";
    }
    comm.cout0() << std::endl;
    comm.barrier();
    if (record_store->num_records() > 0) {
      if (record_store->contains_record(0)) {
        for (const auto& name : series_names) {
          record_store->visit_field(
              name, 0, [](const auto& value) { std::cout << value << "\t\t"; });
        }
        std::cout << std::endl;
      }
    }
  } else if (!parquet_path.empty()) {
    ygm::io::parquet_parser parquetp(comm, {parquet_path}, true);
    const auto&             schema = parquetp.get_schema();
    std::stringstream       headers;
    for (size_t i = 0; i < schema.size(); ++i) {
      headers << schema[i].name << "\t\t";
    }
    comm.cout0(headers.str());
    auto peek = parquetp.peek();
    if (peek) {
      std::stringstream ssline;

      auto visitor = [&ssline](auto field) {
        using T = std::decay_t<decltype(field)>;
        if constexpr (!std::is_same_v<T, std::monostate>) {
          ssline << field << "\t\t";
        }
      };
      for (auto& item : peek.value()) {
        std::visit(visitor, item);
      }
      comm.cout(ssline.str());
    }
  } else {
    comm.cout0("Error: missing required options for peek");
    comm.cout0(peek_options());
    return 1;
  }

  return 0;
}