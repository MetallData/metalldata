#include "subcommand.hpp"
#include <iostream>

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
    return {};
  }

  int run(ygm::comm& comm) override {
    comm.cout0("Erase keys in: ", metall_path);
    metall::utility::metall_mpi_adaptor mpi_adaptor(
        metall::open_only, metall_path, comm.get_mpi_comm());
    auto& manager = mpi_adaptor.get_local_manager();

    static auto* record_store =
        manager.find<record_store_type>(metall::unique_instance).first;

    auto* pm_hash_key = manager.find<persistent_string>("hash_key").first;

    static std::set<std::string> keys_to_erase;
    comm.cf_barrier();
    ygm::io::line_parser lp(comm, {keys_path});
    lp.for_all([&comm, pm_hash_key](const std::string& line) {
      // partition based on primary key
      if (pm_hash_key) {
        int owner = make_hash(line) % comm.size();
        comm.async(
            owner, [](std::string key) { keys_to_erase.insert(key); }, line);
      } else {
        comm.async_bcast([](std::string key) { keys_to_erase.insert(key); },
                         line);
      }
    });
    comm.barrier();

    static std::vector<size_t> records_to_erase;

    std::string_view hname(pm_hash_key->data(), pm_hash_key->size());
    record_store->for_all_dynamic(
        hname, [&comm](const record_store_type::record_id_type index,
                       const auto                              value) {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            if (keys_to_erase.count(std::string(value))) {
              records_to_erase.push_back(index);
              comm.cout("Removing record ", value, " at index ", index);
            }
          } else {
            comm.cerr0("Unsupported hash_key type");
            return;
          }
        });

    for (size_t index : records_to_erase) {
      record_store->remove_record(index);
    }
    keys_to_erase.clear();
    records_to_erase.clear();
    return 0;
  }

 private:
  std::string metall_path;
  std::string keys_path;
};