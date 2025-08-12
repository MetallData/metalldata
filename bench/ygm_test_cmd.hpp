#pragma once

#include "subcommand.hpp"
#include <iostream>
#include <random>

#include <ygm/comm.hpp>

class ygm_test_cmd : public base_subcommand {
 public:
  std::string name() override { return "ygm_test"; }
  std::string desc() override { return "Runs an YGM bandwidth test."; }

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

    // Storage for random dests
    std::vector<int> dests;
    dests.resize(100000000);

    // Generate random dests
    std::mt19937                    gen(comm.rank());
    std::uniform_int_distribution<> distrib(0, comm.size() - 1);
    for (int& v : dests) {
      v = distrib(gen);
    }

    static size_t count_recv(0);
    comm.stats_reset();
    comm.barrier();
    for (const int dest : dests) {
      comm.async(dest, []() { ++count_recv; });
    }
    comm.barrier();

    count_recv = 0;
    comm.barrier();
    comm.stats_reset();
    for (const int dest : dests) {
      comm.async(dest, []() { ++count_recv; });
    }
    comm.barrier();

    comm.stats_print("100M sent per rank");

    return 0;
  }
};