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
#include <ygm/io/parquet2variant.hpp>
#include <ygm/utility.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>

#include <multiseries/multiseries_record.hpp>

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

struct option {
  std::filesystem::path metall_path{"./metall_data"};
  std::string           series_name;
  std::string data_type;  // i: int64_t, u: uint64_t, d: double, s: string
};

bool parse_options(int argc, char *argv[], option *opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:s:t:h")) != -1) {
    switch (opt_char) {
      case 'd':
        opt->metall_path = std::filesystem::path(optarg);
        break;
      case 's':
        opt->series_name = optarg;
        break;
      case 't':
        opt->data_type = optarg;
        break;
      case 'h':
        return false;
    }
  }
  return true;
}

void show_usage(std::ostream &os) {
  os << "Usage: find_max -d metall_path -s series_name -t data_type"
     << std::endl;
  os << "  -d: Path to Metall directory" << std::endl;
  os << "  -s: Series name" << std::endl;
  os << "  -t: Data type (i: int64_t, u: uint64_t, d: double, s: string)"
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
  if (opt.series_name.empty()) {
    comm.cerr0("Series name is required");
    return EXIT_FAILURE;
  }

  metall::utility::metall_mpi_adaptor mpi_adaptor(
      metall::open_read_only, opt.metall_path, comm.get_mpi_comm());
  auto &manager = mpi_adaptor.get_local_manager();
  auto *record_store =
      manager.find<record_store_type>(metall::unique_instance).first;
  if (!record_store) {
    comm.cerr0("Failed to find record store");
    return EXIT_FAILURE;
  }

  comm.cout0() << "Finding max value in series: " << opt.series_name
               << std::endl;
  ygm::timer timer;
  if (opt.data_type == "i") {
    find_max<int64_t>(record_store, opt.series_name, comm);
  } else if (opt.data_type == "u") {
    find_max<uint64_t>(record_store, opt.series_name, comm);
  } else if (opt.data_type == "d") {
    find_max<double>(record_store, opt.series_name, comm);
  } else if (opt.data_type == "s") {
    find_max<std::string>(record_store, opt.series_name, comm);
  } else {
    comm.cerr0() << "Unsupported data type " << opt.data_type << std::endl;
    return EXIT_FAILURE;
  }
  comm.cout0() << "Find max took (s)\t" << timer.elapsed() << std::endl;

  return 0;
}