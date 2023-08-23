// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Run JSON Bento benchmark
/// This benchmark measures the performance and memory usage of storing JSON
/// data in Metall JSON and JSON Bento. This benchmark read JSON files that
/// contain JSON line data.

#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <string_view>
#include <vector>

#include <metall/detail/time.hpp>
#include <metall/metall.hpp>

#include <json_bento/boost_json.hpp>
#include <json_bento/json_bento.hpp>

namespace bj     = boost::json;
namespace mj     = metall::json;
using bento_type = json_bento::box<metall::manager::allocator_type<std::byte>>;

void print_usage(std::string_view program_name);
void parse_options(int argc, char **argv, std::string &metall_datastore_path,
                   std::vector<std::string> &json_line_file_paths);
std::vector<bj::value> read_json_files(
    const std::vector<std::string> &file_paths);
std::vector<std::string> search_file_paths(const std::string_view path);
std::vector<std::string> search_file_paths(
    const std::vector<std::string> &paths);
void execute_command(const std::string_view command);

int main(int argc, char **argv) {
  std::string              metall_datastore_path;
  std::vector<std::string> json_line_file_paths;
  parse_options(argc, argv, metall_datastore_path, json_line_file_paths);

  std::cout << "\n<<Read JSON>>" << std::endl;
  const auto json_lines = read_json_files(json_line_file_paths);

  std::cout << "\n<<Metall JSON>>" << std::endl;
  {
    execute_command("rm -rf " + metall_datastore_path);
    metall::manager manager(metall::create_only, metall_datastore_path.c_str());
    using array_type = mj::array<metall::manager::allocator_type<std::byte>>;
    auto *table      = manager.construct<array_type>(metall::unique_instance)(
        manager.get_allocator());

    const auto start = metall::mtlldetail::elapsed_time_sec();
    table->resize(1);
    for (std::size_t i = 0; i < json_lines.size(); ++i) {
      if (table->size() <= i) {
        table->resize(table->size() * 2);
      }
      (*table)[i] = mj::value_from(json_lines[i], manager.get_allocator());
    }
    std::cout << "Elapsed time (s)\t"
              << metall::mtlldetail::elapsed_time_sec(start) << std::endl;
  }
  execute_command("du -h -d 0 " + metall_datastore_path);

  std::cout << "\n<<JSON Bento>>" << std::endl;
  {
    execute_command("rm -rf " + metall_datastore_path);
    metall::manager manager(metall::create_only, metall_datastore_path.c_str());

    auto *bento = manager.construct<bento_type>(metall::unique_instance)(
        manager.get_allocator());

    const auto start = metall::mtlldetail::elapsed_time_sec();
    for (const auto &line : json_lines) {
      bento->push_back(line);
    }
    std::cout << "Elapsed time (s)\t"
              << metall::mtlldetail::elapsed_time_sec(start) << std::endl;
    // std::cout << "Profile:\n" << std::endl;
    // bento->profile();
  }
  execute_command("du -h -d 0 " + metall_datastore_path);

  // Verification
  // Make sure that the data is stored correctly by comparing the input JSON
  // data and the stored JSON data.
  std::cout << "\nVerification (for JSON Bento)" << std::endl;
  {
    metall::manager manager(metall::open_read_only,
                            metall_datastore_path.c_str());
    const auto *bento = manager.find<bento_type>(metall::unique_instance).first;
    assert(bento);

    if (json_lines.size() != bento->size()) {
      std::cerr << "Wrong size: " << bento->size() << std::endl;
      std::abort();
    }

    for (std::size_t i = 0; i < json_lines.size(); ++i) {
      if (json_bento::value_to<boost::json::value>(bento->at(i)) !=
          json_lines.at(i)) {
        std::cerr << "Different JSON value at " << i << std::endl;

        std::cerr << "-- Input --" << std::endl;
        std::cerr << json_lines.at(i) << std::endl;
        std::cerr << "-- Stored --" << std::endl;
        std::cerr << bento->at(i) << std::endl;

        std::abort();
      }
    }
  }
  std::cout << "Complete!!" << std::endl;

  return 0;
}

std::vector<std::string> search_file_paths(const std::string_view path) {
  std::vector<std::string> paths;
  if (std::filesystem::is_regular_file(std::filesystem::path(path))) {
    paths.emplace_back(path);
  } else {
    for (const auto &entry :
         std::filesystem::recursive_directory_iterator(path)) {
      if (entry.is_regular_file()) paths.emplace_back(entry.path());
    }
  }
  return paths;
}

std::vector<std::string> search_file_paths(
    const std::vector<std::string> &paths) {
  std::vector<std::string> found_paths;
  for (const auto &p : paths) {
    const auto ret = search_file_paths(p);
    found_paths.insert(found_paths.end(), ret.begin(), ret.end());
  }
  return found_paths;
}

void print_usage(std::string_view program_name) {
  std::cout
      << "Usage: " << program_name
      << " [-d Metall datastore path] [Input JSON file/directory paths...]"
      << "\n This program can find JSON files in given directories (no "
         "recursive search)."
      << std::endl;
}

void parse_options(int argc, char **argv, std::string &metall_datastore_path,
                   std::vector<std::string> &json_line_file_paths) {
  int opt;

  while ((opt = getopt(argc, argv, "d:h")) != -1) {
    switch (opt) {
      case 'd':
        metall_datastore_path = optarg;
        break;
      case 'h':
        [[fallthrough]];
      default:
        print_usage(argv[0]);
        std::abort();
    }
  }

  std::vector<std::string> root_paths;
  for (int i = optind; i < argc; ++i) {
    root_paths.push_back(argv[i]);
  }
  json_line_file_paths = search_file_paths(root_paths);

  if (metall_datastore_path.empty() || json_line_file_paths.empty()) {
    print_usage(argv[0]);
    std::abort();
  }

  std::cout << "Metall datastore path: " << metall_datastore_path << std::endl;
  std::cout << "JSON file paths:" << std::endl;
  for (const auto &path : json_line_file_paths) {
    std::cout << "  - " << path << std::endl;
  }
}

void execute_command(const std::string_view command) {
  std::cout << command << std::endl;
  const int  status  = std::system(command.data());
  const bool success = (status != -1) && !!(WIFEXITED(status));
  if (!success) {
    std::cerr << "Failed to execute " << command << std::endl;
  }
}

std::vector<bj::value> read_json_files(
    const std::vector<std::string> &file_paths) {
  std::vector<bj::value> table;
  const auto             start = metall::mtlldetail::elapsed_time_sec();
  std::size_t            count = 0;
  for (const auto &f : file_paths) {
    // std::cout << "Open " << f << std::endl;

    std::ifstream ifs(f);
    if (!ifs.is_open()) {
      std::cerr << "Failed to open " << f << std::endl;
      std::abort();
    }

    std::string buf;
    while (std::getline(ifs, buf)) {
      table.emplace_back(bj::parse(buf));
      ++count;
    }
  }
  const auto elapsed_time = metall::mtlldetail::elapsed_time_sec(start);
  std::cout << "#of read lines\t" << count << std::endl;
  std::cout << "Elapsed time (s)\t" << elapsed_time << std::endl;
  return table;
}