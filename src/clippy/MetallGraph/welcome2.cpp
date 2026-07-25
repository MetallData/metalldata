// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>

static const std::string method_name = "welcome2";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Prints YGM's welcome message"};

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  for (size_t i = 0; i < 10; ++i) {
    comm.cerr0() << "Here is line " << i << std::endl;
    comm.cerr0() << "Here is another line for " << i
                 << std::endl;  // << std::endl;
    comm.cerr0() << "And a third line for " << i << std::endl;
    comm.cerr0() << "And a fourth line for " << i << std::endl;
    comm.cerr0() << "And a multi line\nthat will test\nembedded lines for " << i
                 << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  comm.cerr0() << "We're all done!" << std::endl;

  clip.to_return(0);
  return 0;
} catch (const std::runtime_error& e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
