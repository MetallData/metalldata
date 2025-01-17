// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <unistd.h>

#include <filesystem>
#include <iostream>

#include <boost/json/src.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/stopwatch.h>

#include "utils.hpp"

struct option {
  std::filesystem::path input_path;
  std::filesystem::path metall_path{"./metall_data"};
  std::vector<std::string> inclusive_keys;
  std::vector<std::string> discard_values;
};

// Overload for std::ostream
std::ostream &operator<<(std::ostream &os, const option &opt) {
  os << "input_path: " << opt.input_path.string() << std::endl;
  os << "metall_path: " << opt.metall_path.string() << std::endl;
  os << "inclusive_keys: " << std::endl;
  for (const auto &key : opt.inclusive_keys) {
    os << "  " << key << std::endl;
  }
  os << "discard_values: " << std::endl;
  for (const auto &value : opt.discard_values) {
    os << "  " << value << std::endl;
  }
  return os;
}

// Parse command line options
bool parse_options(int argc, char *argv[], option *opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:i:k:D:")) != -1) {
    switch (opt_char) {
    case 'd':
      opt->metall_path = std::filesystem::path(optarg);
      break;
    case 'i':
      opt->input_path = std::filesystem::path(optarg);
      break;
    case 'k': {
      std::string keys(optarg);
      std::string delimiter = ":";
      size_t pos = 0;
      while ((pos = keys.find(delimiter)) != std::string::npos) {
        opt->inclusive_keys.push_back(keys.substr(0, pos));
        keys.erase(0, pos + delimiter.length());
      }
      opt->inclusive_keys.push_back(keys);
      break;
    }
    case 'D': {
      std::string keys(optarg);
      std::string delimiter = ":";
      size_t pos = 0;
      while ((pos = keys.find(delimiter)) != std::string::npos) {
        opt->discard_values.push_back(keys.substr(0, pos));
        keys.erase(0, pos + delimiter.length());
      }
      opt->discard_values.push_back(keys);
      break;
    }
    default:
      std::cerr << "Invalid option" << std::endl;
      return false;
    }
  }

  if (opt->metall_path.empty()) {
    std::cerr << "Metall path is required" << std::endl;
    return false;
  }

  if (opt->input_path.empty()) {
    std::cerr << "Input path is required" << std::endl;
    return false;
  }

  return true;
}

// Read strings from files
// Apply the given procedure to each line
template <typename StrProcedure>
size_t read_string(const std::vector<std::filesystem::path> &file_paths,
                   StrProcedure str_procedure) {
  size_t num_lines = 0;
  for (const auto &file_path : file_paths) {
    std::ifstream ifs(file_path);
    if (!ifs.is_open()) {
      std::cerr << "Failed to open file: " << file_path << std::endl;
      continue;
    }

    std::string line;
    while (std::getline(ifs, line)) {
      str_procedure(line);
      ++num_lines;
    }
  }
  return num_lines;
}

template <typename KVProcedure>
size_t
read_ndjson_string_values(const std::vector<std::filesystem::path> &file_paths,
                          const KVProcedure &kv_procedure) {
  // Each line is a json object
  // Parse JSON object and pass it to kv_procedure
  return read_string(file_paths, [&kv_procedure](const std::string &line) {
    const auto json = boost::json::parse(line);
    for (const auto &json_kv : json.as_object()) {
      if (json_kv.value().is_string()) {
        kv_procedure(json_kv.key(), json_kv.value().as_string());
      }
    }
  });
}

/// Parse reddit data (NDJSON) and pass key-value pairs to the KVInserter.
/// All entries with non-string values are discarded.
template <typename KVInserter>
void run_reddit_bench(const std::filesystem::path &input_path,
                      const KVInserter &kv_inserter) {
  const auto input_file_paths = find_files(input_path);
  spdlog::info("Read {} files", input_file_paths.size());

  spdlog::info("Start bench");
  spdlog::stopwatch sw;
  read_ndjson_string_values(input_file_paths, kv_inserter);
  spdlog::info("Elapsed time: {} seconds", sw);
}

// Check if the line is any of the inclusive keys.
// If inclusive_keys is empty, return true.
bool include_string(const std::string &line,
                    const std::vector<std::string> &inclusive_keys) {
  if (inclusive_keys.empty()) {
    return true;
  }

  for (const auto &inclusive_key : inclusive_keys) {
    if (line.find(inclusive_key) != std::string::npos) {
      return true;
    }
  }
  return false;
}

// Check if the line is any of the discard values.
bool exclude_string(const std::string &line,
                    const std::vector<std::string> &discard_values) {
  for (const auto &discard_value : discard_values) {
    if (line == discard_value) {
      return true;
    }
  }
  return false;
}