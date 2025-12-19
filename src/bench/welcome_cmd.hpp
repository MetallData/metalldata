#pragma once

#include "subcommand.hpp"
#include <iostream>

#include <ygm/comm.hpp>

class welcome_cmd : public base_subcommand {
 public:
  std::string name() override { return "welcome"; }
  std::string desc() override { return "Prints a welcome screen."; }

  boost::program_options::options_description get_options() override {
    namespace po = boost::program_options;
    po::options_description od;
    return od;
  }

  std::string parse(const boost::program_options::variables_map&) override {
    return {};
  }

  int run(ygm::comm& comm) override {
    comm.welcome();
    return 0;
  }
};