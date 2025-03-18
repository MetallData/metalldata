// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <limits>
#include <chrono>

#include <metall/metall.hpp>

#include <multiseries/multiseries_record.hpp>

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

struct option {
  std::filesystem::path    metall_path{"./metall_data"};
  std::vector<std::string> series_names;
};

std::vector<std::string> parse_csv(const std::string &csv) {
  std::vector<std::string> result;
  std::string              token;
  std::istringstream       token_stream(csv);
  while (std::getline(token_stream, token, ',')) {
    result.push_back(token);
  }
  return result;
}

bool parse_options(int argc, char *argv[], option *opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:s:h")) != -1) {
    switch (opt_char) {
      case 'd':
        opt->metall_path = std::filesystem::path(optarg);
        break;
      case 's':
        opt->series_names = parse_csv(optarg);
        break;
      case 'h':
        return false;
    }
  }
  return true;
}

void show_usage(std::ostream &os) {
  os << "Usage: find_max -d metall path -s series names -t data types"
     << std::endl;
  os << "  -d: Path to Metall directory" << std::endl;
  os << "  -s: Series name(s), separated by comma, e.g., name,age" << std::endl;
}

auto start_timer() {
  return std::chrono::high_resolution_clock::now();
}

auto get_elapsed_time_seconds(const std::chrono::time_point<std::chrono::high_resolution_clock>& start) {
  const auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
}

int main(int argc, char *argv[]) {

  option opt;
  if (!parse_options(argc, argv, &opt)) {
    show_usage(std::cerr);
    return EXIT_SUCCESS;
  }
  if (opt.metall_path.empty()) {
    std::cerr << "Metall path is required" << std::endl;
    return EXIT_FAILURE;
  }
  if (opt.series_names.empty()) {
    std::cerr << "Series name is required" << std::endl;
    return EXIT_FAILURE;
  }

  metall::manager manager(metall::open_read_only, opt.metall_path);
  auto *record_store =
      manager.find<record_store_type>(metall::unique_instance).first;
  if (!record_store) {
    std::cerr << "Failed to find record store in " + opt.metall_path.string()
              << std::endl;
    return EXIT_FAILURE;
  }

  for (size_t i = 0; i < opt.series_names.size(); ++i) {
    const auto &series_name = opt.series_names[i];
    if (!record_store->contains(series_name)) {
      std::cerr << "Series not found: " << series_name << std::endl;
      continue;
    }

    std::cerr << "Finding max value in series: " << series_name << std::endl;
    const auto timer = start_timer();

    struct max_value_t {
      int64_t     i = std::numeric_limits<int64_t>::min();
      uint64_t    u = std::numeric_limits<uint64_t>::min();
      double      d = std::numeric_limits<double>::min();
      std::string s;
      bool        fi = false;
      bool        fu = false;
      bool        fd = false;
      bool        fs = false;
    };
    static max_value_t max_value;
    max_value = max_value_t(); // reset

    record_store->for_all_dynamic(
        series_name,
        [](const record_store_type::record_id_type, const auto value) {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, int64_t>) {
            max_value.i  = std::max(max_value.i, value);
            max_value.fi = true;
          } else if constexpr (std::is_same_v<T, uint64_t>) {
            max_value.u  = std::max(max_value.u, value);
            max_value.fu = true;
          } else if constexpr (std::is_same_v<T, double>) {
            max_value.d  = std::max(max_value.d, value);
            max_value.fd = true;
          } else if constexpr (std::is_same_v<T, std::string_view>) {
            if (max_value.s.empty() ||
                std::lexicographical_compare(max_value.s.begin(),
                                             max_value.s.end(), value.begin(),
                                             value.end())) {
              max_value.s = std::string(value);
            }
            max_value.fs = true;
          } else {
            std::cerr << "Unsupported data type " << value << std::endl;
          }
        });
    const auto elapsed_time = get_elapsed_time_seconds(timer);
    std::cout << "Max value in series: " << series_name << std::endl;
    std::cout << "Elapsed time: " << elapsed_time << " seconds" << std::endl;
    if (max_value.fi) {
      std::cerr << "Max value: " << max_value.i << std::endl;
    } else if (max_value.fu) {
      std::cerr << "Max value: " << max_value.u << std::endl;
    } else if (max_value.fd) {
      std::cerr << "Max value: " << max_value.d << std::endl;
    } else if (max_value.fs) {
      std::cerr << "Max value: " << max_value.s << std::endl;
    }
    }

  return 0;
}