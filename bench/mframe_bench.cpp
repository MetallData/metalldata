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
#include <ygm/io/line_parser.hpp>
#include <ygm/container/set.hpp>
#include <ygm/utility.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <multiseries/multiseries_record.hpp>
#include <boost/program_options.hpp>

#include "mframe_bench.hpp"
#include "distinct.hpp"
#include "peek.hpp"
#include "gen_uuids.hpp"
#include <ygm/progress_indicator.hpp>

void print_command_help(const char*                    argv0,
                        const po::options_description& global,
                        const po::options_description& ingest,
                        const po::options_description& rm_options,
                        const po::options_description& erase_keys_options) {
  std::cout << std::endl
            << "Usage: " << argv0 << " <command> [options]" << std::endl
            << std::endl;
  std::cout << "Commands:" << std::endl;
  std::cout << ingest << std::endl;
  std::cout << rm_options << std::endl;
  std::cout << peek_options() << std::endl;
  std::cout << erase_keys_options << std::endl;
  std::cout << distinct_options() << std::endl;
}

void run_erase_keys(ygm::comm& comm, std::string& metall_path,
                    std::string& keys_path) {
  comm.cout0("Erase keys in: ", metall_path);
  metall::utility::metall_mpi_adaptor mpi_adaptor(
      metall::open_only, metall_path, comm.get_mpi_comm());
  auto& manager = mpi_adaptor.get_local_manager();

  static auto* record_store =
      manager.find<record_store_type>(metall::unique_instance).first;

  auto* pm_hash_key = manager.find<persistent_string>("hash_key").first;

  static std::set<std::string> keys_to_erase;
  comm.cf_barrier();
  ygm::io::line_parser lp(comm, {keys_path});
  lp.for_all([&comm](const std::string& line) {
    // partition based on primary key
    int owner = make_hash(line) % comm.size();
    comm.async(owner, [](std::string key) { keys_to_erase.insert(key); }, line);
  });
  comm.barrier();

  static std::vector<size_t> records_to_erase;

  std::string_view hname(pm_hash_key->data(), pm_hash_key->size());
  record_store->for_all_dynamic(
      hname,
      [&comm](const record_store_type::record_id_type index, const auto value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string_view>) {
          if (keys_to_erase.count(std::string(value))) {
            records_to_erase.push_back(index);
            comm.cout("Removing record ", value, " at index ", index);
          }
        } else {
          comm.cerr0("Unsupported hash_key type");
          return;
        }
      });

  for (size_t index : records_to_erase) {
    record_store->remove_record(index);
  }
  keys_to_erase.clear();
  records_to_erase.clear();
}

void run_ingest(ygm::comm& comm, const std::string& input_path,
                const std::string& metall_path, const std::string& hash_key,
                bool recursive) {
  comm.cout0("Ingest from: ", input_path, " into ", metall_path,
             " key: ", hash_key, " recursive: ", recursive);

  if (std::filesystem::exists(metall_path)) {
    comm.cerr0(
        "Metall path already exists, it must be manually removed with 'rm' "
        "command");
    return;
  }
  comm.cf_barrier();

  ygm::timer                          setup_timer;
  metall::utility::metall_mpi_adaptor mpi_adaptor(
      metall::create_only, metall_path, comm.get_mpi_comm());
  auto& manager = mpi_adaptor.get_local_manager();

  auto* string_store = manager.construct<string_store_type>(
      metall::unique_instance)(manager.get_allocator());
  static auto* record_store = manager.construct<record_store_type>(
      metall::unique_instance)(string_store, manager.get_allocator());

  ygm::io::parquet_parser parquetp(comm, {input_path}, recursive);
  const auto&             schema = parquetp.get_schema();

  //
  // Locate index of primary key
  int primary_key_index = -1;
  for (size_t i = 0; i < schema.size(); ++i) {
    if (hash_key == schema[i].name) {
      comm.cerr0("Found primary key: ", i);
      primary_key_index = i;
      break;
    }
  }
  if (primary_key_index == -1) {
    comm.cerr0("Primary key not found: ", hash_key);
    return;
  }

  auto* pm_hash_key = manager.construct<persistent_string>("hash_key")(
      hash_key.c_str(), manager.get_allocator());

  // Add series
  static std::vector<size_t> vec_col_ids;
  for (const auto& s : schema) {
    if (s.type.equal(parquet::Type::INT32) ||
        s.type.equal(parquet::Type::INT64)) {
      auto series_index = record_store->add_series<int64_t>(s.name);
      vec_col_ids.push_back(series_index);
    } else if (s.type.equal(parquet::Type::FLOAT) or
               s.type.equal(parquet::Type::DOUBLE)) {
      auto series_index = record_store->add_series<double>(s.name);
      vec_col_ids.push_back(series_index);
    } else if (s.type.equal(parquet::Type::BYTE_ARRAY)) {
      auto series_index = record_store->add_series<std::string_view>(s.name);
      vec_col_ids.push_back(series_index);
    } else {
      comm.cerr0() << "Unsupported column type: " << s.type << std::endl;
      MPI_Abort(comm.get_mpi_comm(), EXIT_FAILURE);
    }
  }
  comm.cf_barrier();
  comm.cout0() << "Setup took (s): " << setup_timer.elapsed() << std::endl;

  ygm::timer    ingest_timer;
  static size_t total_ingested_str_size = 0;
  static size_t total_ingested_bytes    = 0;
  static size_t total_num_strs          = 0;
  static bool   bprofile                = false;
  parquetp.for_all([&schema, primary_key_index, &comm](auto&& row) {
    auto record_inserter = [](auto&& row) {
      const auto record_id = record_store->add_record();
      for (int i = 0; i < row.size(); ++i) {
        auto& field = row[i];
        if (std::holds_alternative<std::monostate>(field)) {
          continue;  // Leave the field empty for None/NaN values
        }
        // const auto &name = schema[i].name;
        size_t name = vec_col_ids[i];
        std::visit(
            [&record_id, &name](auto&& field) {
              using T = std::decay_t<decltype(field)>;
              if constexpr (std::is_same_v<T, int32_t> ||
                            std::is_same_v<T, int64_t>) {
                record_store->set<int64_t>(name, record_id, field);
                if (bprofile) {
                  total_ingested_bytes += sizeof(T);
                }
              } else if constexpr (std::is_same_v<T, float> ||
                                   std::is_same_v<T, double>) {
                record_store->set<double>(name, record_id, field);
                if (bprofile) {
                  total_ingested_bytes += sizeof(T);
                }
              } else if constexpr (std::is_same_v<T, std::string>) {
                record_store->set<std::string_view>(name, record_id, field);

                if (bprofile) {
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
    };

    // partition based on primary key
    int owner = std::visit([](auto&& field) { return make_hash(field); },
                           row[primary_key_index]) %
                comm.size();
    // std::cout << "owner = " << owner << std::endl;
    comm.async(owner, record_inserter, row);
  });
  comm.barrier();
  comm.cout0() << "Ingest took (s): " << ingest_timer.elapsed() << std::endl;

  size_t total_unique_str_size = 0;
  if (bprofile) {
    for (const auto& str : *string_store) {
      total_unique_str_size += str.length();
    }
  }

  comm.cout0() << "#of series: " << record_store->num_series() << std::endl;
  comm.cout0() << "#of records: " << ygm::sum(record_store->num_records(), comm)
               << std::endl;

  comm.cout0() << "Series name, Load factor" << std::endl;
  for (const auto s : schema) {
    // comm.cout("record_store->load_factor(s.name) = ",
    // record_store->load_factor(s.name));
    const auto ave_load_factor =
        ygm::sum(record_store->load_factor(s.name), comm) / comm.size();
    comm.cout0() << "  " << s.name << ", " << ave_load_factor << std::endl;
  }

  if (bprofile) {
    comm.cout0() << "Total ingested bytes: "
                 << ygm::sum(total_ingested_bytes, comm) << std::endl;
    comm.cout0() << "Total #of ingested chars: "
                 << ygm::sum(total_ingested_str_size, comm) << std::endl;
    comm.cout0() << "Total bytes of ingested numbers: "
                 << ygm::sum(total_ingested_bytes - total_ingested_str_size,
                             comm)
                 << std::endl;
    comm.cout0() << "#of unique strings: "
                 << ygm::sum(string_store->size(), comm) << std::endl;
    comm.cout0() << "Total #of chars of unique strings: "
                 << ygm::sum(total_unique_str_size, comm) << std::endl;
    comm.cout0() << "Metall datastore size (only the path rank 0 can access):"
                 << std::endl;
    comm.cout0() << get_dir_usage(metall_path) << std::endl;
  }
}

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  po::options_description global("Global options");
  global.add_options()("command", po::value<std::string>(),
                       "Subcommand to execute")("help", "Display help message");

  po::positional_options_description pos;
  pos.add("command", 1);

  po::options_description ingest("ingest:   ingests from parquet");
  ingest.add_options()("input_path", po::value<std::string>(),
                       "Path to parquet input")(
      "recursive", po::value<bool>()->implicit_value(false),
      "read input path recursively")("metall_path", po::value<std::string>(),
                                     "Path to Metall storage")(
      "hash_key", po::value<std::string>(), "Semi-unique record key");

  po::options_description rm_options("rm:  deletes metall data");
  rm_options.add_options()("metall_path", po::value<std::string>(),
                           "Path to Metall storage");

  po::options_description erase_keys_options(
      "erase_keys:  erases all matching keys");
  erase_keys_options.add_options()("metall_path", po::value<std::string>(),
                                   "Path to Metall storage");
  erase_keys_options.add_options()("keys_path", po::value<std::string>(),
                                   "Path to input text file of keys");

  po::variables_map vm;

  try {
    po::parsed_options parsed = po::command_line_parser(argc, argv)
                                    .options(global)
                                    .positional(pos)
                                    .allow_unregistered()
                                    .run();

    po::store(parsed, vm);

    po::notify(vm);

    if (vm.count("command")) {
      std::string command = vm["command"].as<std::string>();
      if (command == "ingest") {
        po::store(po::parse_command_line(argc, argv, ingest), vm);
        po::notify(vm);

        if (vm.count("input_path") && vm.count("metall_path") &&
            vm.count("hash_key")) {
          std::string input_path  = vm["input_path"].as<std::string>();
          std::string metall_path = vm["metall_path"].as<std::string>();
          std::string hash_key    = vm["hash_key"].as<std::string>();
          bool        recursive   = vm.count("recursive");

          run_ingest(world, input_path, metall_path, hash_key, recursive);

        } else {
          world.cout0("Error: missing required options for ingest");
          world.cout0(ingest);
          return 1;
        }
      } else if (command == "rm") {
        po::store(po::parse_command_line(argc, argv, rm_options), vm);
        po::notify(vm);

        if (vm.count("metall_path")) {
          std::string metall_path = vm["metall_path"].as<std::string>();
          if (!std::filesystem::exists(metall_path)) {
            world.cerr0("Not found: ", metall_path);
            return 1;
          }
          world.cf_barrier();
          world.cout0("Removing: ", metall_path);
          std::filesystem::remove_all(metall_path);
        } else {
          world.cout0("Error: missing required options for rm");
          world.cout0(rm_options);
          return 1;
        }
      } else if (command == "peek") {
        po::store(po::parse_command_line(argc, argv, peek_options()), vm);
        po::notify(vm);

        return run_peek(world, vm);

      } else if (command == "erase_keys") {
        po::store(po::parse_command_line(argc, argv, erase_keys_options), vm);
        po::notify(vm);

        if (vm.count("metall_path") && vm.count("keys_path")) {
          std::string metall_path = vm["metall_path"].as<std::string>();
          std::string keys_path   = vm["keys_path"].as<std::string>();
          if (!std::filesystem::exists(metall_path)) {
            world.cerr0("Not found: ", metall_path);
            return 1;
          }
          if (!std::filesystem::exists(keys_path)) {
            world.cerr0("Not found: ", keys_path);
            return 1;
          }
          world.cf_barrier();
          run_erase_keys(world, metall_path, keys_path);
        } else {
          world.cout0("Error: missing required options for peek");
          world.cout0(erase_keys_options);
          return 1;
        }
      } else if (command == "distinct") {
        po::store(po::parse_command_line(argc, argv, distinct_options()), vm);
        po::notify(vm);
        return run_distinct(world, vm);
      } else if (command == "gen_uuids") {
        po::store(po::parse_command_line(argc, argv, gen_uuids_options()), vm);
        po::notify(vm);
        return run_gen_uuids(world, vm);
      } else {
        if (world.rank0()) {
          print_command_help(argv[0], global, ingest, rm_options,
                             erase_keys_options);
        }
        return 1;
      }
    } else {
      if (world.rank0()) {
        print_command_help(argv[0], global, ingest, rm_options,
                           erase_keys_options);
      }
      return 0;
    }
  } catch (const po::error& ex) {
    world.cout0("Options error: ", ex.what());
    return 1;
  }

  return 0;
}