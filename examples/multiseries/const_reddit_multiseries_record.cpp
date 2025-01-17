// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/json/src.hpp>
#include <metall/metall.hpp>
#include <spdlog/spdlog.h>

#include <multiseries/multiseries_record.hpp>

#include "utils.hpp"

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

struct option {
  std::filesystem::path metall_path{"./metall_data"};
  std::filesystem::path input_path;
};

bool parse_options(int argc, char *argv[], option *opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:i:")) != -1) {
    switch (opt_char) {
    case 'd':
      opt->metall_path = std::filesystem::path(optarg);
      break;
    case 'i':
      opt->input_path = std::filesystem::path(optarg);;
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
    return 1;
  }


  metall::manager manager(metall::create_only, opt.metall_path);

  auto *string_store = manager.construct<string_store_type>(
      metall::unique_instance)(manager.get_allocator());
  auto *record_store = manager.construct<record_store_type>(
      metall::unique_instance)(string_store, manager.get_allocator());

  const auto autor = record_store->add_series<std::string_view>("author");
  const auto parent_id =
      record_store->add_series<std::string_view>("parent_id");
  const auto subbreddit =
      record_store->add_series<std::string_view>("subreddit");
  const auto body = record_store->add_series<std::string_view>("body");
  const auto created_utc = record_store->add_series<uint64_t>("created_utc");

  for (const auto &file : find_files(opt.input_path)) {
    spdlog::info("Reading file: {}", file.string());
    std::ifstream ifs(file);
    std::string line;
    while (std::getline(ifs, line)) {
      const auto json = boost::json::parse(line);
      const auto record_id = record_store->add_record();
      for (const auto &json_kv : json.as_object()) {
        const auto &key = json_kv.key();
        if (key == "author") {
          record_store->set<std::string_view>(autor, record_id,
                                              json_kv.value().as_string());
        } else if (key == "parent_id") {
          record_store->set<std::string_view>(parent_id, record_id,
                                              json_kv.value().as_string());
        } else if (key == "subreddit") {
          record_store->set<std::string_view>(subbreddit, record_id,
                                              json_kv.value().as_string());
        } else if (key == "body") {
          record_store->set<std::string_view>(body, record_id,
                                              json_kv.value().as_string());
        } else if (key == "created_utc") {
          // The parser does not support uint64_t
          if (json_kv.value().is_int64()) {
            record_store->set<uint64_t>(created_utc, record_id,
                                        json_kv.value().as_int64());
          } else if (json_kv.value().is_string()) {
            record_store->set<uint64_t>(
                created_utc, record_id,
                std::stoull(json_kv.value().as_string().c_str()));
          } else {
            spdlog::error("Unexpected value type for created_utc: {}", line);
          }
        }
      }
    }
  }

  spdlog::info("#of series: {}", record_store->num_series());
  spdlog::info("#of records: {}", record_store->num_records());
  spdlog::info("Metall directory size: {}", get_dir_usage(opt.metall_path));

  return 0;
}