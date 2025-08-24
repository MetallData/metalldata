#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/static_string/static_string.hpp>
#include "metall/utility/metall_mpi_adaptor.hpp"
#include "mframe_bench.hpp"
#include "subcommand.hpp"
#include "ygm/io/line_parser.hpp"
#include "ygm/utility/world.hpp"
#include <optional>
#include <ygm/comm.hpp>

// todo:   Add optional extra parameter to be series name to delete when
// "hash_key" is missing from the metall store.

class erase_keys_cmd : public base_subcommand {
 public:
  std::string name() override { return "erase_keys"; }
  std::string desc() override { return "Erases columns by provided keys."; }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()("metall_path", po::value<std::string>(),
                     "Path to Metall storage");
    od.add_options()("keys_path", po::value<std::string>(),
                     "Path to input text file of keys");
    od.add_options()("hash_key", po::value<std::string>(),
                     "Name of hash key for local partitioning (required if "
                     "hash key not already specified)");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    if (vm.count("metall_path") && vm.count("keys_path")) {
      metall_path = vm["metall_path"].as<std::string>();
      keys_path   = vm["keys_path"].as<std::string>();

      if (!std::filesystem::exists(metall_path)) {
        return std::string("Not found: ") + metall_path;
      }
      if (!std::filesystem::exists(keys_path)) {
        return std::string("Not found: ") + keys_path;
      }
    } else {
      return "Error: missing required options for peek";
    }

    if (vm.count("hash_key")) {
      hash_key = vm["hash_key"].as<std::string>();
      ygm::wcout0("Got hash_key ", hash_key.value(), ".");
    }

    return {};
  }

  int run(ygm::comm& comm) override {
    comm.cout0("Erase keys in: ", metall_path);
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    auto* pm_hash_key = manager.find<persistent_string>("hash_key").first;
    if (!hash_key.has_value() && !pm_hash_key) {
      comm.cerr0("Must specify hash_key or have set it via ingest");
      return 1;
    }

    bool local_partition = hash_key.has_value() && !pm_hash_key;
    comm.cout0("Partitioning: ", local_partition ? "local" : "distributed");
    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;

    std::string hash_key_unwrapped;
    if (local_partition) {
      hash_key_unwrapped = hash_key.value();
    } else {
      auto* pm_hash_key  = manager.find<persistent_string>("hash_key").first;
      hash_key_unwrapped = {pm_hash_key->cbegin(), pm_hash_key->cend()};
    }

    // static std::set<std::string> keys_to_erase;
    using string40 = boost::static_strings::static_string<40>;
    static boost::unordered::unordered_flat_set<string40> keys_to_erase;
    comm.cf_barrier();
    ygm::io::line_parser lp(comm, {keys_path});
    lp.for_all([&comm, local_partition](const std::string& line) {
      // partition based on primary key
      if (!local_partition) {
        int owner = make_hash(line) % comm.size();
        comm.async(
            owner, [](std::string key) { keys_to_erase.insert(string40(key)); },
            line);
      } else {
        comm.async_bcast(
            [](std::string key) { keys_to_erase.insert(string40(key)); }, line);
      }
    });

    comm.barrier();

    static std::vector<size_t> records_to_erase;
    record_store->for_all_dynamic(
        hash_key_unwrapped,
        [&comm](const record_store_type::record_id_type index,
                const auto                              value) {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            if (keys_to_erase.contains(string40(value))) {
              records_to_erase.push_back(index);
            }
          } else {
            comm.cout0("Oops. value = ", value);
            comm.cerr0("Unsupported hash_key type");
            // return;
          }
        });

    comm.cout0(ygm::sum(records_to_erase.size(), comm),
               " entries to be removed.");
    for (size_t index : records_to_erase) {
      record_store->remove_record(index);
    }
    keys_to_erase.clear();
    records_to_erase.clear();
    return 0;
  }

 private:
  std::string                metall_path;
  std::string                keys_path;
  std::optional<std::string> hash_key = std::nullopt;
};