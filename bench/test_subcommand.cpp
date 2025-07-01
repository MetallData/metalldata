#include "subcommand.hpp"
#include <iostream>

#include <ygm/comm.hpp>

class welcome_command : public base_subcommand {
 public:
  std::string name() override { return "welcome"; }
  std::string desc() override { return "Prints YGM welcome screen."; }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    return od;
  }

  std::string parse(const boost::program_options::variables_map& vm) override {
    return {};
  }

  int run(ygm::comm& comm) override {
    comm.welcome();
    return 0;
  }
};

int main(int argc, char** argv) {
  ygm::comm      world(&argc, &argv);
  cli_subcommand cli(world);
  cli.add_subcommand<welcome_command>();

  return cli.run(argc, argv);
}