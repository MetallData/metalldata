// Copyright 2025 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#ifndef METALL_DISABLE_CONCURRENCY
#define METALL_DISABLE_CONCURRENCY
#endif

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

#include <ygm/comm.hpp>
#include <ygm/io/detail/parquet2variant.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <multiseries/multiseries_record.hpp>

#include "utils.hpp"

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

struct option {
  std::filesystem::path metall_path{"./metall_data"};
  std::filesystem::path input_path;
};

bool parse_options(int argc, char* argv[], option* opt) {
  int opt_char;
  while ((opt_char = getopt(argc, argv, "d:i:")) != -1) {
    switch (opt_char) {
      case 'd':
        opt->metall_path = std::filesystem::path(optarg);
        break;
      case 'i':
        opt->input_path = std::filesystem::path(optarg);
        ;
        break;
    }
  }
  return true;
}

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  option opt;
  if (!parse_options(argc, argv, &opt)) {
    std::abort();
  }
  if (opt.metall_path.empty()) {
    comm.cerr0() << "Metall path is required" << std::endl;
    return 1;
  }

  metall::utility::metall_mpi_adaptor mpi_adaptor(
      metall::create_only, opt.metall_path, comm.get_mpi_comm());
  auto& manager = mpi_adaptor.get_local_manager();

  auto* string_store = manager.construct<string_store_type>(
      metall::unique_instance)(manager.get_allocator());
  auto* record_store = manager.construct<record_store_type>(
      metall::unique_instance)(string_store, manager.get_allocator());

  ygm::io::parquet_parser parquetp(comm, {opt.input_path});
  const auto&             schema = parquetp.schema();

  // Add series
  for (const auto& [type, name] : schema) {
    if (type.equal(parquet::Type::INT32) || type.equal(parquet::Type::INT64)) {
      record_store->add_series<int64_t>(name);
    } else if (type.equal(parquet::Type::FLOAT) or
               type.equal(parquet::Type::DOUBLE)) {
      record_store->add_series<double>(name);
    } else if (type.equal(parquet::Type::BYTE_ARRAY)) {
      record_store->add_series<std::string_view>(name);
    } else {
      comm.cerr0() << "Unsupported column type: " << type << std::endl;
      MPI_Abort(comm.get_mpi_comm(), EXIT_FAILURE);
    }
  }

  parquetp.for_all([&schema, &record_store](auto& stream_reader, const auto&) {
    const auto record_id = record_store->add_record();
    auto row = ygm::io::detail::read_parquet_as_variant(stream_reader, schema);
    for (int i = 0; i < row.size(); ++i) {
      auto& field = row[i];
      if (std::holds_alternative<std::monostate>(field)) {
        continue;  // Skip invalid data
      }

      const auto& name = std::get<1>(schema[i]);
      std::visit(
          [&record_store, &record_id, &name](auto&& field) {
            using T = std::decay_t<decltype(field)>;
            if constexpr (std::is_same_v<T, int32_t> ||
                          std::is_same_v<T, int64_t>) {
              record_store->set<int64_t>(name, record_id, field);
            } else if constexpr (std::is_same_v<T, float> ||
                                 std::is_same_v<T, double>) {
              record_store->set<double>(name, record_id, field);
            } else if constexpr (std::is_same_v<T, std::string>) {
              record_store->set<std::string_view>(name, record_id, field);
            } else {
              throw std::runtime_error("Unsupported type");
            }
          },
          std::move(field));
    }
  });

  comm.cout0() << "#of series: " << record_store->num_series() << std::endl;
  comm.cout0() << "#of records: " << comm.all_reduce_sum(record_store->num_records()) << std::endl;
  comm.cout0() << "Metall datastore size (rank 0):" << std::endl;
  comm.cout0() << get_dir_usage(opt.metall_path) << std::endl;

  return 0;
}