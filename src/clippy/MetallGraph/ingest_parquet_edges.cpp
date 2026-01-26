// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include <format>
#include "utils.hpp"

static const std::string method_name    = "ingest_parquet_edges";
static const std::string state_name     = "INTERNAL";
static const std::string log_state_name = "loglevel";

int main(int argc, char **argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Reads a parquet file of edge data"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required_state<int>(log_state_name,
                               "Log level (as Python logging integer)");
  clip.add_required<std::string>("input_path", "Path to parquet input");
  clip.add_required<std::string>("col_u", "Edge U column name");
  clip.add_required<std::string>("col_v", "Edge V column name");
  clip.add_optional<bool>("directed",
                          "True if edges are directed (default true)", true);
  clip.add_optional<std::vector<std::string>>(
    "metadata", "Column names of additional fields to ingest", {});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path       = clip.get_state<std::string>("path");
  auto loglevel   = clip.get_state<int>("loglevel");

  comm.set_logger_target(ygm::logger_target::stderr);
  comm.set_log_level(metalldata::loglevel_py2ygm(loglevel));

  auto input_path = clip.get<std::string>("input_path");
  auto col_u      = clip.get<std::string>("col_u");
  auto col_v      = clip.get<std::string>("col_v");
  auto directed   = clip.get<bool>("directed");
  auto meta_str   = clip.get<std::vector<std::string>>("metadata");

  metalldata::metall_graph mg(comm, path, false);

  std::vector<metalldata::metall_graph::series_name> meta;
  meta.reserve(meta_str.size());

  bool has_meta = clip.has_argument("metadata");

  for (const auto m : meta_str) {
    meta.emplace_back("edge", m);
  }

  auto rc =
    has_meta
      ? mg.ingest_parquet_edges(input_path, true, col_u, col_v, directed, meta)
      : mg.ingest_parquet_edges(input_path, true, col_u, col_v, directed);

  if (!rc.good()) {
    comm.cerr0(rc.error);
    return -1;
  }

  for (const auto &[warn, count] : rc.warnings) {
    comm.cerr0(std::format("{} : {}", warn, count));
  }

  clip.update_selectors(mg.get_selector_info());

  // TODO: the return_info dict vals are std::any. This needs explicit JSON
  // serialization if we want to return info from it.
  //   clip.to_return(rc.return_info);
  return 0;
}