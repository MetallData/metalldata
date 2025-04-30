// Copyright 2025 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

/// Demonstration of how the multiseries containers work.

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <filesystem>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include <metall/metall.hpp>
#include <multiseries/multiseries_record.hpp>
#include "utils.hpp"

using namespace multiseries;

void parse_option(int argc,
                  char *argv[],
                  std::filesystem::path &metall_path,
                  size_t &num_records) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:n:")) != -1) {
    switch (opt_char) {
      case 'd':
        metall_path = std::filesystem::path(optarg);
        break;
      case 'n':
        num_records = std::stoull(optarg);
        break;
    }
  }
}

using record_store_type = basic_record_store<metall::manager::allocator_type<
  std::byte> >;
using string_store_type = record_store_type::string_store_type;

template<typename data_type, typename generator_type>
void run_bench(const std::filesystem::path &metall_path,
               const size_t num_records,
               const container_kind kind,
               generator_type &&generator) {
  metall::manager manager(metall::create_only, metall_path);

  auto *string_store = manager.construct<string_store_type>(
    metall::unique_instance)(manager.get_allocator());
  auto *record_store = manager.construct<record_store_type>(
    metall::unique_instance)(string_store, manager.get_allocator());

  record_store->add_series<data_type>("data", kind);
  for (int64_t i = 0; i < num_records; ++i) {
    const auto record_id = record_store->add_record();
    record_store->set<data_type>("data", record_id, generator());
  }

  std::cout << "Total #of records: " << record_store->num_records() <<
      std::endl;
  std::cout << "#of unique strings: " << string_store->size() << std::endl;
  std::cout << get_dir_usage(metall_path) << std::endl;
}

int main(int argc, char **argv) {
  std::filesystem::path metall_path{"./metall_data"};
  size_t num_records = 1'000'000;
  parse_option(argc, argv, metall_path, num_records);

  std::cout << "Ingest bool values" << std::endl;
  std::cout << "Dense container" << std::endl;
  run_bench<bool>(metall_path,
                  num_records,
                  container_kind::dense,
                  []() { return bool(std::rand() % 2); });

  std::cout << "Sparse container" << std::endl;
  run_bench<bool>(metall_path,
                  num_records,
                  container_kind::sparse,
                  []() { return bool(std::rand() % 2); });

  std::cout << "----------" << std::endl;

  std::cout << "Ingest int64_t values" << std::endl;
  std::cout << "Dense container" << std::endl;
  run_bench<int64_t>(metall_path,
                     num_records,
                     container_kind::dense,
                     []() { return std::rand(); });

  std::cout << "Sparse container" << std::endl;
  run_bench<int64_t>(metall_path,
                     num_records,
                     container_kind::sparse,
                     []() { return std::rand(); });

  std::cout << "----------" << std::endl;

  std::cout << "Ingest UUIDs" << std::endl;
  boost::uuids::random_generator gen;
  std::cout << "Sample UUID: " << boost::uuids::to_string(gen()) << std::endl;
  std::cout << "Dense container" << std::endl;
  run_bench<std::string_view>(metall_path,
                              num_records,
                              container_kind::dense,
                              [&gen]() -> std::string {
                                return boost::uuids::to_string(gen());
                              });
  std::cout << "Sparse container" << std::endl;
  run_bench<std::string_view>(metall_path,
                              num_records,
                              container_kind::sparse,
                              [&gen]() -> std::string {
                                return boost::uuids::to_string(gen());
                              });

  return 0;
}
