#include "mframe_bench.hpp"
#include "subcommand.hpp"
#include "ygm/io/parquet_parser.hpp"
#include <iostream>
#include <string>

#include <ygm/comm.hpp>
#include <ygm/utility/timer.hpp>
#include <ygm/utility/progress_indicator.hpp>

#include <metall/utility/metall_mpi_adaptor.hpp>

class partition_test_cmd : public base_subcommand {
 public:
  std::string name() override { return "partition_test"; }
  std::string desc() override {
    return "Tests the partitioning performance without storing in metall";
  }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()("input_path", po::value<std::string>(),
                     "Path to parquet input")(
        "recursive", po::value<bool>()->implicit_value(false),
        "read input path recursively")("hash_key", po::value<std::string>(),
                                       "Semi-unique record key");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (vm.count("input_path")) {
      input_path = vm["input_path"].as<std::string>();
      recursive  = vm.count("recursive");
    } else {
      return "Error: missing required options for ingest";
    }
    if (vm.count("hash_key")) {
      hash_key = vm["hash_key"].as<std::string>();
    } else {
      return "Error: missing required options for ingest";
    }
    return {};
  }

  int run(ygm::comm& comm) override {
    auto hash_key_unwrapped = *hash_key;
    comm.cout0("Partition Test from: ", input_path,
               " key: ", hash_key_unwrapped, " recursive: ", recursive);

    ygm::io::parquet_parser parquetp(comm, {input_path}, recursive);
    const auto&             schema = parquetp.get_schema();

    //
    // Locate index of primary key
    static int primary_key_index = -1;
    for (size_t i = 0; i < schema.size(); ++i) {
      if (hash_key_unwrapped == schema[i].name) {
        comm.cerr0("Found primary key: ", i);
        primary_key_index = i;
        break;
      }
    }
    if (primary_key_index == -1) {
      comm.cerr0("Primary key not found: ", hash_key_unwrapped);
      return 0;
    }

    comm.cf_barrier();

    ygm::utility::timer ingest_timer;
    static size_t       local_records_ingested = 0;

    ygm::utility::progress_indicator pi(
        comm, {.update_freq = 100, .message = "Records ingested"});
    parquetp.for_all([&schema, &comm, &pi](auto&& row) {
      pi.async_inc();
      auto record_inserter = [](auto&& row) {
        for (int i = 0; i < row.size(); ++i) {
          local_records_ingested++;
        };
      };

      // partition based on primary key
      int owner = std::visit([](auto&& field) { return make_hash(field); },
                             row[primary_key_index]) %
                  comm.size();
      comm.async(owner, record_inserter, row);
    });
    pi.complete();
    comm.barrier();
    comm.cout0("DONE, ignore progress meter above");
    comm.cout0("Records ingested: ", ygm::sum(local_records_ingested, comm));
    comm.cout0("Ingest took (s): ", ingest_timer.elapsed());

    return 0;
  }

 private:
  std::string                input_path;
  std::optional<std::string> hash_key = std::nullopt;
  bool                       recursive;
};
