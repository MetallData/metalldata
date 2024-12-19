// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <iostream>

#include <metall/container/vector.hpp>
#include <metall/metall.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/stopwatch.h>

#include <string_table/string_store.hpp>
#include <string_table/string_vector.hpp>

#include "reddit_bench_common.hpp"

using store_type =
    compact_string::string_store<metall::manager::allocator_type<std::byte>>;
using string_vector_type =
    compact_string::vector<metall::manager::allocator_type<std::byte>>;

int main(int argc, char *argv[]) {

  option opt;
  if (!parse_options(argc, argv, &opt)) {
    std::abort();
  }
  std::cout << opt << std::endl;

  metall::manager manager(metall::create_only, opt.metall_path);

  auto *string_store = manager.construct<store_type>(metall::unique_instance)(
      manager.get_allocator());
  auto *string_vector = manager.construct<string_vector_type>(
      metall::unique_instance)(string_store, manager.get_allocator());

  size_t total_string_size = 0;
  run_reddit_bench(opt.input_path, [&opt, &string_vector, &total_string_size](
                                       const auto &key, const auto &value) {
    if (!include_string(key, opt.inclusive_keys)) {
      return;
    }
    if (exclude_string(value.c_str(), opt.discard_values)) {
      return;
    }
    string_vector->push_back(value.c_str(), value.size());
    total_string_size += value.size();
  });
  spdlog::info("#of all items: {}", string_vector->size());
  spdlog::info("total #of string chars: {}", total_string_size);
  spdlog::info("#of entries in string store (not #of unique long strings): {}", string_store->size());
  spdlog::info("Directory size: {}", get_dir_usage(opt.metall_path));

  return 0;
}