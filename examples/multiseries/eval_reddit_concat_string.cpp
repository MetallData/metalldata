// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <metall/metall.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/stopwatch.h>

#include "reddit_bench_common.hpp"

int main(int argc, char *argv[]) {

  option opt;
  if (!parse_options(argc, argv, &opt)) {
    std::abort();
  }
  std::cout << opt << std::endl;

  metall::manager manager(metall::create_only, opt.metall_path);

  std::unordered_set<std::string> string_table;
  size_t total_string_size = 0;
  run_reddit_bench(
      opt.input_path, [&opt, &string_table, &total_string_size,
                       &manager](const auto &key, const auto &value) {
        if (!include_string(key, opt.inclusive_keys)) {
          return;
        }
        if (exclude_string(value.c_str(), opt.discard_values)) {
          return;
        }
        if (string_table.find(value.c_str()) == string_table.end()) {
          total_string_size += value.size();
          string_table.insert(value.c_str());
          auto *s = static_cast<char *>(manager.allocate(value.size()));
          std::memcpy(s, value.c_str(), value.size());
        }
      });

  spdlog::info("#of unique items: {}", string_table.size());
  spdlog::info("Total unique string size: {}", total_string_size);
  spdlog::info("Directory size: {}", get_dir_usage(opt.metall_path));

  //manager.profile(&std::cout);

  return 0;
}