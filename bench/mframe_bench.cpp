// Copyright 2025 Lawrence Livermore National Security, LLC and other MetallData
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
#include <ygm/utility/timer.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <multiseries/multiseries_record.hpp>
#include <boost/program_options.hpp>

#include "mframe_bench.hpp"
#include "distinct_cmd.hpp"
#include "peek_cmd.hpp"
#include "gen_uuids_cmd.hpp"
#include "gen_faker_cmd.hpp"  // Add new include
#include "subcommand.hpp"
#include "welcome_cmd.hpp"
#include "rm_cmd.hpp"
#include "ingest_cmd.hpp"
#include "erase_keys_cmd.hpp"
#include "remove_if_cmd.hpp"
#include "remove_if2_cmd.hpp"
#include "ygm_test_cmd.hpp"
#include "partition_test_cmd.hpp"
#include "count_cmd.hpp"
#include "filter_to_parquet_cmd.hpp"
#include <ygm/utility/progress_indicator.hpp>

int main(int argc, char** argv) {
  ygm::comm      world(&argc, &argv);
  cli_subcommand cli(world);
  cli.add_subcommand<welcome_cmd>();
  cli.add_subcommand<ingest_cmd>();
  cli.add_subcommand<rm_cmd>();
  cli.add_subcommand<erase_keys_cmd>();
  cli.add_subcommand<peek_cmd>();
  cli.add_subcommand<distinct_cmd>();
  cli.add_subcommand<gen_uuids_cmd>();
  cli.add_subcommand<gen_faker_cmd>();  // Add new subcommand
  cli.add_subcommand<remove_if_cmd>();
  cli.add_subcommand<remove_if2_cmd>();
  cli.add_subcommand<ygm_test_cmd>();
  cli.add_subcommand<partition_test_cmd>();
  cli.add_subcommand<count_cmd>();
  cli.add_subcommand<filter_to_parquet_cmd>();

  return cli.run(argc, argv);
}