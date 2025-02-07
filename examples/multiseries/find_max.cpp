// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <algorithm>
#include <iostream>
#include <string>
#include <limits>

#include <mpi.h>
#include <ygm/comm.hpp>
#include <ygm/utility.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>

#include <multiseries/multiseries_record.hpp>

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

struct option {
  std::filesystem::path    metall_path{"./metall_data"};
  std::vector<std::string> series_names;
  std::vector<std::string>
      data_types;  // i: int64_t, u: uint64_t, d: double, s: string
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
  while ((opt_char = getopt(argc, argv, "d:s:t:h")) != -1) {
    switch (opt_char) {
      case 'd':
        opt->metall_path = std::filesystem::path(optarg);
        break;
      case 's':
        opt->series_names = parse_csv(optarg);
        break;
      case 't':
        opt->data_types = parse_csv(optarg);
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
  os << "  -t: Data type(s) (i: int64_t, u: uint64_t, d: double, s: string) "
        "for the series, separated by comma, e.g., i,u"
     << std::endl;
}

template <typename T>
void find_max(const record_store_type *const record_store,
              const std::string &series_name, ygm::comm &comm) {
  T max_value = std::numeric_limits<T>::min();
  record_store->for_all<T>(
      series_name,
      [&max_value](const record_store_type::record_id_type, const auto value) {
        max_value = std::max(max_value, value);
      });
  comm.cout0() << "Max value: " << comm.all_reduce_max(max_value) << std::endl;
}

// find_max for string
template <>
void find_max<std::string>(const record_store_type *const record_store,
                           const std::string &series_name, ygm::comm &comm) {
  std::string max_value;
  record_store->for_all<std::string_view>(
      series_name,
      [&max_value](const record_store_type::record_id_type, const auto value) {
        if (max_value.empty() ||
            std::lexicographical_compare(max_value.begin(), max_value.end(),
                                         value.begin(), value.end())) {
          max_value = std::string(value);
        }
      });
  comm.cout0() << "Lexicographically max value: "
               << comm.all_reduce(max_value,
                                  [](const auto &lhd, const auto &rhd) {
                                    return std::lexicographical_compare(
                                               lhd.begin(), lhd.end(),
                                               rhd.begin(), rhd.end())
                                               ? rhd
                                               : lhd;
                                  })
               << std::endl;
}

int main(int argc, char *argv[]) {
  ygm::comm comm(&argc, &argv);

  option opt;
  if (!parse_options(argc, argv, &opt)) {
    show_usage(comm.cerr0());
    return EXIT_SUCCESS;
  }
  if (opt.metall_path.empty()) {
    comm.cerr0("Metall path is required");
    return EXIT_FAILURE;
  }
  if (opt.series_names.empty()) {
    comm.cerr0("Series name is required");
    return EXIT_FAILURE;
  }
  if (opt.data_types.empty()) {
    comm.cerr0("Data type is required");
    return EXIT_FAILURE;
  }
  if (opt.series_names.size() != opt.data_types.size()) {
    comm.cerr0("Series names and data types must have the same size");
    return EXIT_FAILURE;
  }

  metall::utility::metall_mpi_adaptor mpi_adaptor(
      metall::open_read_only, opt.metall_path, comm.get_mpi_comm());
  auto &manager = mpi_adaptor.get_local_manager();
  auto *record_store =
      manager.find<record_store_type>(metall::unique_instance).first;
  if (!record_store) {
    comm.cerr0("Failed to find record store in " + opt.metall_path.string());
    return EXIT_FAILURE;
  }

  for (size_t i = 0; i < opt.series_names.size(); ++i) {
    const auto &series_name = opt.series_names[i];
    const auto &data_type   = opt.data_types[i];
    if (!record_store->contains(series_name)) {
      comm.cerr0() << "Series not found: " << series_name << std::endl;
      continue;
    }

    comm.cout0() << "Finding max value in series: " << series_name << std::endl;
    ygm::timer timer;
    if (data_type == "i") {
      find_max<int64_t>(record_store, series_name, comm);
    } else if (data_type == "u") {
      find_max<uint64_t>(record_store, series_name, comm);
    } else if (data_type == "d") {
      find_max<double>(record_store, series_name, comm);
    } else if (data_type == "s") {
      find_max<std::string>(record_store, series_name, comm);
    } else {
      comm.cerr0() << "Unsupported data type " << data_type << std::endl;
      return EXIT_FAILURE;
    }
    comm.cout0() << "Find max took (s)\t" << timer.elapsed() << "\n"
                 << std::endl;
  }

  return 0;
}