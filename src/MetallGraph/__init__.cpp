// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <boost/json.hpp>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>
#include <ygm/utility.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <multiseries/multiseries_record.hpp>

namespace boostjsn = boost::json;

static const std::string method_name = "__init__";
static const std::string state_name = "INTERNAL";
static const std::string sel_state_name = "selectors";


using record_store_type =
multiseries::basic_record_store<metall::manager::allocator_type<std::byte> >;
using string_store_type = record_store_type::string_store_type;


int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Initializes a MetallGraph"};
  clip.add_required<std::string>("path", "Storage path for MetallGraph");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  std::string path = clip.get<std::string>("path");
  clip.set_state("path", path);

  //todo: check if already exists and open existing
  metall::utility::metall_mpi_adaptor mpi_adaptor(
    metall::create_only,
    path,
    comm.get_mpi_comm());
  auto &manager = mpi_adaptor.get_local_manager();

  auto *string_store = manager.construct<string_store_type>(
    metall::unique_instance)(manager.get_allocator());
  auto *record_store = manager.construct<record_store_type>(
    metall::unique_instance)(string_store, manager.get_allocator());

  return 0;
}