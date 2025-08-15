// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <boost/json.hpp>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <multiseries/multiseries_record.hpp>

namespace boostjsn = boost::json;

static const std::string method_name    = "read_edges";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

using record_store_type = multiseries::basic_record_store<
    metall::manager::allocator_type<std::byte> >;
using string_store_type = record_store_type::string_store_type;

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Initializes a MetallGraph"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("input_path", "Path to parquet input");
  clip.add_required<std::string>("u_col", "Edge U column name");
  clip.add_required<std::string>("v_col", "Edge V column name");

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  std::string path       = clip.get_state<std::string>("path");
  std::string input_path = clip.get<std::string>("input_path");
  std::string u_col      = clip.get<std::string>("u_col");
  std::string v_col      = clip.get<std::string>("v_col");

  clip.set_state<std::string>("path", path);
  clip.set_state<std::string>("u_col", u_col);
  clip.set_state<std::string>("v_col", v_col);

  // // todo: check if doesn't exist and fail
  // metall::utility::metall_mpi_adaptor mpi_adaptor(metall::open_only, path,
  //                                                 comm.get_mpi_comm());

  // auto &manager = mpi_adaptor.get_local_manager();

  // auto *string_store =
  //     manager.find<string_store_type>(metall::unique_instance).first;
  // auto *record_store =
  //     manager.find<record_store_type>(metall::unique_instance).first;

  // //
  // //
  // ygm::io::parquet_parser parquetp(comm, {input_path});
  // const auto             &schema = parquetp.schema();

  // // Add series
  // for (const auto &[type, name] : schema) {
  //   if (type.equal(parquet::Type::INT32) || type.equal(parquet::Type::INT64))
  //   {
  //     record_store->add_series<int64_t>(name);
  //   } else if (type.equal(parquet::Type::FLOAT) or
  //              type.equal(parquet::Type::DOUBLE)) {
  //     record_store->add_series<double>(name);
  //   } else if (type.equal(parquet::Type::BYTE_ARRAY)) {
  //     record_store->add_series<std::string_view>(name);
  //   } else {
  //     comm.cerr0() << "Unsupported column type: " << type << std::endl;
  //     MPI_Abort(comm.get_mpi_comm(), EXIT_FAILURE);
  //   }
  // }
  // comm.cf_barrier();

  // parquetp.for_all([&schema, &record_store](auto&& row) {
  //   const auto record_id = record_store->add_record();
  //   for (int i = 0; i < row.size(); ++i) {
  //     auto &field = row[i];
  //     if (std::holds_alternative<std::monostate>(field)) {
  //       continue;  // Leave the field empty for None/NaN values
  //     }

  //     const auto &name = std::get<1>(schema[i]);
  //     std::visit(
  //         [&record_store, &record_id, &name](auto &&field) {
  //           using T = std::decay_t<decltype(field)>;
  //           if constexpr (std::is_same_v<T, int32_t> ||
  //                         std::is_same_v<T, int64_t>) {
  //             record_store->set<int64_t>(name, record_id, field);
  //           } else if constexpr (std::is_same_v<T, float> ||
  //                                std::is_same_v<T, double>) {
  //             record_store->set<double>(name, record_id, field);
  //           } else if constexpr (std::is_same_v<T, std::string>) {
  //             record_store->set<std::string_view>(name, record_id, field);
  //           } else {
  //             throw std::runtime_error("Unsupported type");
  //           }
  //         },
  //         std::move(field));
  //   }
  // });
  // comm.barrier();

  clip.set_state<std::string>("path", path);
  clip.set_state<std::string>("u_col", u_col);
  clip.set_state<std::string>("v_col", v_col);

  return 0;
}