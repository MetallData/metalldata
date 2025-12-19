#pragma once

#include "subcommand.hpp"
#include <random>

#include <ygm/comm.hpp>
#include <ygm/utility/timer.hpp>

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

    comm.stats_print(
        "To compute All-to-all bandwidth divide isend_bytes by elapsed time.");

    static size_t hop_count = 0;
    static auto&  scomm     = comm;
    comm.barrier();
    ygm::utility::timer atw_test;

    struct around_the_world {
      void operator()() {
        if (hop_count++ < 100) {
          int next_rank = (scomm.rank() + 1) % scomm.size();
          scomm.async(next_rank, around_the_world{});
        }
      }
    };

    if (comm.rank0()) {
      int next_rank = (scomm.rank() + 1) % scomm.size();
      scomm.async(next_rank, around_the_world{});
    }
    comm.local_wait_until([]() { return hop_count == 100; });
    comm.barrier();
    comm.cout0("Around the world hop latency: ",
               atw_test.elapsed() / 100.0 / comm.size() * 1000000.0, " us");

    return 0;
  }
};