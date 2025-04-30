// Copyright 2025 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>
#include <ygm/utility.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <multiseries/multiseries_record.hpp>

#include "utils.hpp"

using record_store_type = multiseries::basic_record_store<
    metall::manager::allocator_type<std::byte> >;
using string_store_type = record_store_type::string_store_type;

struct option {
  std::filesystem::path metall_path{"./metall_data"};
  std::filesystem::path input_path;
  bool                  profile{false};
};

bool parse_options(int argc, char *argv[], option *opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:i:Ph")) != -1) {
    switch (opt_char) {
      case 'd':
        opt->metall_path = std::filesystem::path(optarg);
        break;
      case 'i':
        opt->input_path = std::filesystem::path(optarg);
        break;
      case 'P':
        opt->profile = true;
        break;
      case 'h':
        return false;
    }
  }
  return true;
}

// Generic make_hash function using std::hash
template <typename T>
size_t make_hash(const T &value) {
  std::hash<T> hasher;
  return hasher(value);
}

void show_usage(std::ostream &os) {
  os << "Usage: ingest_parquet -d metall_path -i input_path" << std::endl;
  os << "  -d: Path to Metall directory" << std::endl;
  os << "  -i: Path to an input Parquet file or directory contains Parquet "
        "files" << std::endl;
  os << "  -P: Enable profiling (may harm speed)" << std::endl;
}

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  option opt;
  if (!parse_options(argc, argv, &opt)) {
    show_usage(comm.cerr0());
    return EXIT_SUCCESS;
  }
  if (opt.metall_path.empty()) {
    comm.cerr0() << "Metall path is required" << std::endl;
    return 1;
  }

  ygm::timer                          setup_timer;
  metall::utility::metall_mpi_adaptor mpi_adaptor(
      metall::create_only, opt.metall_path, comm.get_mpi_comm());
  auto &manager = mpi_adaptor.get_local_manager();

  auto *string_store = manager.construct<string_store_type>(
      metall::unique_instance)(manager.get_allocator());
  auto *record_store = manager.construct<record_store_type>(
      metall::unique_instance)(string_store, manager.get_allocator());

  ygm::io::parquet_parser parquetp(comm, {opt.input_path});
  const auto             &schema = parquetp.get_schema();

  // Add series
  for (const auto &s : schema) {
    if (s.type.equal(parquet::Type::INT32) || s.type.equal(parquet::Type::INT64)) {
      record_store->add_series<int64_t>(s.name);
    } else if (s.type.equal(parquet::Type::FLOAT) or
               s.type.equal(parquet::Type::DOUBLE)) {
      record_store->add_series<double>(s.name);
    } else if (s.type.equal(parquet::Type::BYTE_ARRAY)) {
      record_store->add_series<std::string_view>(s.name);
    } else {
      comm.cerr0() << "Unsupported column type: " << s.type << std::endl;
      MPI_Abort(comm.get_mpi_comm(), EXIT_FAILURE);
    }
  }
  record_store->add_series<bool>("__deleted");
  comm.cf_barrier();
  comm.cout0() << "Setup took (s): " << setup_timer.elapsed() << std::endl;

  ygm::timer    ingest_timer;
  static size_t total_ingested_str_size = 0;
  static size_t total_ingested_bytes    = 0;
  static size_t total_num_strs          = 0;
  parquetp.for_all(
      [&schema, &record_store, &opt](const auto& row) {
        const auto record_id = record_store->add_record();
        for (int i = 0; i < row.size(); ++i) {
          auto &field = row[i];
          if (std::holds_alternative<std::monostate>(field)) {
            continue;  // Leave the field empty for None/NaN values
          }

          const auto &name = schema[i].name;
          std::visit(
              [&record_store, &record_id, &name, &opt](auto &&field) {
                using T = std::decay_t<decltype(field)>;
                if constexpr (std::is_same_v<T, int32_t> ||
                              std::is_same_v<T, int64_t>) {
                  record_store->set<int64_t>(name, record_id, field);
                  if (opt.profile) {
                    total_ingested_bytes += sizeof(T);
                  }
                } else if constexpr (std::is_same_v<T, float> ||
                                     std::is_same_v<T, double>) {
                  record_store->set<double>(name, record_id, field);
                  if (opt.profile) {
                    total_ingested_bytes += sizeof(T);
                  }
                } else if constexpr (std::is_same_v<T, std::string>) {
                  record_store->set<std::string_view>(name, record_id, field);

                  if (opt.profile) {
                    total_ingested_str_size += field.size();
                    total_ingested_bytes += field.size();  // Assume ASCII
                    ++total_num_strs;
                  }
                } else {
                  throw std::runtime_error("Unsupported type");
                }
              },
              std::move(field));
        }
      });
  comm.barrier();
  comm.cout0() << "Ingest took (s): " << ingest_timer.elapsed() << std::endl;

  size_t total_unique_str_size = 0;
  if (opt.profile) {
    for (const auto &str : *string_store) {
      total_unique_str_size += str.length();
    }
  }

  comm.cout0() << "#of series: " << record_store->num_series() << std::endl;
  comm.cout0() << "#of records: "
               << comm.all_reduce_sum(record_store->num_records()) << std::endl;

  comm.cout0() << "Series name, Load factor" << std::endl;
  for (const auto &s: schema) {
    const auto ave_load_factor =
        comm.all_reduce_sum(record_store->load_factor(s.name)) / comm.size();
    comm.cout0() << "  " << s.name << ", " << ave_load_factor << std::endl;
  }

  if (opt.profile) {
    comm.cout0() << "Total ingested bytes: "
                 << comm.all_reduce_sum(total_ingested_bytes) << std::endl;
    comm.cout0() << "Total #of ingested chars: "
                 << comm.all_reduce_sum(total_ingested_str_size) << std::endl;
    comm.cout0() << "Total bytes of ingested numbers: "
                 << comm.all_reduce_sum(total_ingested_bytes -
                                        total_ingested_str_size)
                 << std::endl;
    comm.cout0() << "#of unique strings: "
                 << comm.all_reduce_sum(string_store->size()) << std::endl;
    comm.cout0() << "Total #of chars of unique strings: "
                 << comm.all_reduce_sum(total_unique_str_size) << std::endl;
    comm.cout0() << "Metall datastore size (only the path rank 0 can access):"
                 << std::endl;
    comm.cout0() << get_dir_usage(opt.metall_path) << std::endl;
  }

  return 0;
}
