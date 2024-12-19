// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#define METALL_DISABLE_CONCURRENCY

#include <algorithm>
#include <iostream>
#include <string>

#include <metall/metall.hpp>
#include <spdlog/spdlog.h>

#include <multiseries/multiseries_record.hpp>

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

struct option {
  std::filesystem::path metall_path{"./metall_data"};
  std::string series_name;
};

bool parse_options(int argc, char *argv[], option *opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:s:")) != -1) {
    switch (opt_char) {
    case 'd':
      opt->metall_path = std::filesystem::path(optarg);
      break;
    case 's':
      opt->series_name = optarg;
      break;
    }
  }
  return true;
}

int main(int argc, char *argv[]) {

  option opt;
  if (!parse_options(argc, argv, &opt)) {
    std::abort();
  }
  if (opt.metall_path.empty()) {
    std::cerr << "Metall path is required" << std::endl;
    return EXIT_SUCCESS;
  }
  if (opt.series_name.empty()) {
    std::cerr << "Series name is required" << std::endl;
    return EXIT_SUCCESS;
  }

  metall::manager manager(metall::open_read_only, opt.metall_path);
  auto *record_store =
      manager.find<record_store_type>(metall::unique_instance).first;
  if (!record_store) {
    spdlog::error("Failed to find record store");
    return EXIT_SUCCESS;
  }

  spdlog::info("Finding max value in series: {}", opt.series_name);
  if (opt.series_name == "created_utc") {
    uint64_t max_value = 0;
    record_store->for_all<uint64_t>(
        opt.series_name, [&max_value](const record_store_type ::record_id_type,
                                      const auto value) {
          max_value = std::max(max_value, value);
        });
    spdlog::info("Max value: {}", max_value);
  } else {
    std::string max_value;
    record_store->for_all<std::string_view>(
        opt.series_name, [&max_value](const record_store_type ::record_id_type,
                                      const auto value) {
          if (max_value.empty() ||
              std::lexicographical_compare(max_value.begin(), max_value.end(),
                                           value.begin(), value.end())) {
            max_value = std::string(value);
          }
        });
    spdlog::info("Lexicographically max value: {}", max_value);
  }

  return 0;
}