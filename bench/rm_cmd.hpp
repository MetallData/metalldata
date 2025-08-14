#include "subcommand.hpp"
#include <iostream>
#include <string>

#include <ygm/comm.hpp>

class rm_cmd : public base_subcommand {
 public:
  std::string name() override { return "rm"; }
  std::string desc() override { return "Removes a metall dataframe."; }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    od.add_options()("metall_path", po::value<std::string>()->required(),
                     "Path to Metall storage");
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    metall_path = vm["metall_path"].as<std::string>();
    if (!std::filesystem::exists(metall_path)) {
      return std::string("Not found: ") + metall_path;
    }
    return {};
  }

  int run(ygm::comm& comm) override {
    comm.cout0("Removing: ", metall_path);
    std::filesystem::remove_all(metall_path);
    return 0;
  }

 private:
  std::string metall_path;
};