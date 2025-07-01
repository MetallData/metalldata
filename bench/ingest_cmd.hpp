#include "subcommand.hpp"
#include <iostream>
#include <string>

#include <ygm/comm.hpp>
#include <ygm/utility/timer.hpp>
#include <ygm/utility/progress_indicator.hpp>

class ingest_cmd : public base_subcommand {
 public:
  std::string name() override { return "ingest"; }
  std::string desc() override { return "Ingests parquet into metall."; }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()("input_path", po::value<std::string>(),
                     "Path to parquet input")(
        "recursive", po::value<bool>()->implicit_value(false),
        "read input path recursively")("metall_path", po::value<std::string>(),
                                       "Path to Metall storage")(
        "hash_key", po::value<std::string>(), "Semi-unique record key");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (vm.count("input_path") && vm.count("metall_path") &&
        vm.count("hash_key")) {
      input_path  = vm["input_path"].as<std::string>();
      metall_path = vm["metall_path"].as<std::string>();
      hash_key    = vm["hash_key"].as<std::string>();
      recursive   = vm.count("recursive");
    } else {
      return "Error: missing required options for ingest";
    }
    if (std::filesystem::exists(metall_path)) {
      return "Metall path already exists, it must be manually removed with "
             "'rm' command";
    }
    return {};
  }

  int run(ygm::comm& comm) override {
    comm.cout0("Ingest from: ", input_path, " into ", metall_path,
               " key: ", hash_key, " recursive: ", recursive);
    ygm::utility::timer                 setup_timer;
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::create_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    auto* string_store = manager.construct<string_store_type>(
        metall::unique_instance)(manager.get_allocator());
    static auto* record_store = manager.construct<record_store_type>(
        metall::unique_instance)(string_store, manager.get_allocator());

    ygm::io::parquet_parser parquetp(comm, {input_path}, recursive);
    const auto&             schema = parquetp.get_schema();

    //
    // Locate index of primary key
    int primary_key_index = -1;
    for (size_t i = 0; i < schema.size(); ++i) {
      if (hash_key == schema[i].name) {
        comm.cerr0("Found primary key: ", i);
        primary_key_index = i;
        break;
      }
    }
    if (primary_key_index == -1) {
      comm.cerr0("Primary key not found: ", hash_key);
      return 0;
    }

    auto* pm_hash_key = manager.construct<persistent_string>("hash_key")(
        hash_key.c_str(), manager.get_allocator());

    // Add series
    static std::vector<size_t> vec_col_ids;
    for (const auto& s : schema) {
      if (s.type.equal(parquet::Type::INT32) ||
          s.type.equal(parquet::Type::INT64)) {
        auto series_index = record_store->add_series<int64_t>(s.name);
        vec_col_ids.push_back(series_index);
      } else if (s.type.equal(parquet::Type::FLOAT) or
                 s.type.equal(parquet::Type::DOUBLE)) {
        auto series_index = record_store->add_series<double>(s.name);
        vec_col_ids.push_back(series_index);
      } else if (s.type.equal(parquet::Type::BYTE_ARRAY)) {
        auto series_index = record_store->add_series<std::string_view>(s.name);
        vec_col_ids.push_back(series_index);
      } else {
        comm.cerr0() << "Unsupported column type: " << s.type << std::endl;
        MPI_Abort(comm.get_mpi_comm(), EXIT_FAILURE);
      }
    }
    comm.cf_barrier();
    comm.cout0() << "Setup took (s): " << setup_timer.elapsed() << std::endl;

    ygm::utility::timer              ingest_timer;
    static size_t                    total_ingested_str_size = 0;
    static size_t                    total_ingested_bytes    = 0;
    static size_t                    total_num_strs          = 0;
    static bool                      bprofile                = false;
    ygm::utility::progress_indicator pi(
        comm, {.update_freq = 100, .message = "Records ingested"});
    parquetp.for_all([&schema, primary_key_index, &comm, &pi](auto&& row) {
      pi.async_inc();
      auto record_inserter = [](auto&& row) {
        const auto record_id = record_store->add_record();
        for (int i = 0; i < row.size(); ++i) {
          auto& field = row[i];
          if (std::holds_alternative<std::monostate>(field)) {
            continue;  // Leave the field empty for None/NaN values
          }
          // const auto &name = schema[i].name;
          size_t name = vec_col_ids[i];
          std::visit(
              [&record_id, &name](auto&& field) {
                using T = std::decay_t<decltype(field)>;
                if constexpr (std::is_same_v<T, int32_t> ||
                              std::is_same_v<T, int64_t>) {
                  record_store->set<int64_t>(name, record_id, field);
                  if (bprofile) {
                    total_ingested_bytes += sizeof(T);
                  }
                } else if constexpr (std::is_same_v<T, float> ||
                                     std::is_same_v<T, double>) {
                  record_store->set<double>(name, record_id, field);
                  if (bprofile) {
                    total_ingested_bytes += sizeof(T);
                  }
                } else if constexpr (std::is_same_v<T, std::string>) {
                  record_store->set<std::string_view>(name, record_id, field);

                  if (bprofile) {
                    total_ingested_str_size += field.size();
                    total_ingested_bytes += field.size();  // Assume ASCII
                    ++total_num_strs;
                  }
                } else {
                  throw std::runtime_error("Unsupported type");
                }
              },
              std::move(field));
        }
      };

      // partition based on primary key
      int owner = std::visit([](auto&& field) { return make_hash(field); },
                             row[primary_key_index]) %
                  comm.size();
      // std::cout << "owner = " << owner << std::endl;
      comm.async(owner, record_inserter, row);
    });
    pi.complete();
    comm.barrier();
    comm.cout0() << "Ingest took (s): " << ingest_timer.elapsed() << std::endl;

    size_t total_unique_str_size = 0;
    if (bprofile) {
      for (const auto& str : *string_store) {
        total_unique_str_size += str.length();
      }
    }

    comm.cout0() << "#of series: " << record_store->num_series() << std::endl;
    comm.cout0() << "#of records: "
                 << ygm::sum(record_store->num_records(), comm) << std::endl;

    comm.cout0() << "Series name, Load factor" << std::endl;
    for (const auto s : schema) {
      // comm.cout("record_store->load_factor(s.name) = ",
      // record_store->load_factor(s.name));
      const auto ave_load_factor =
          ygm::sum(record_store->load_factor(s.name), comm) / comm.size();
      comm.cout0() << "  " << s.name << ", " << ave_load_factor << std::endl;
    }

    if (bprofile) {
      comm.cout0() << "Total ingested bytes: "
                   << ygm::sum(total_ingested_bytes, comm) << std::endl;
      comm.cout0() << "Total #of ingested chars: "
                   << ygm::sum(total_ingested_str_size, comm) << std::endl;
      comm.cout0() << "Total bytes of ingested numbers: "
                   << ygm::sum(total_ingested_bytes - total_ingested_str_size,
                               comm)
                   << std::endl;
      comm.cout0() << "#of unique strings: "
                   << ygm::sum(string_store->size(), comm) << std::endl;
      comm.cout0() << "Total #of chars of unique strings: "
                   << ygm::sum(total_unique_str_size, comm) << std::endl;
      comm.cout0() << "Metall datastore size (only the path rank 0 can access):"
                   << std::endl;
      comm.cout0() << get_dir_usage(metall_path) << std::endl;
    }
    return 0;
  }

 private:
  std::string input_path;
  std::string metall_path;
  std::string hash_key;
  bool        recursive;
};