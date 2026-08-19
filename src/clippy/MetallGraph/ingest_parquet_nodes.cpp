// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <format>
#include <metalldata/metall_graph.hpp>
#include <stdexcept>
#include <ygm/comm.hpp>

#include "utils.hpp"

static const std::string method_name = "ingest_parquet_nodes";
static const std::string log_state_name = "loglevel";

int main(int argc, char** argv) try {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Reads node metadata from a parquet file"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required_state<int>(log_state_name,
                               "Log level (as Python logging integer)");
  clip.add_required<std::string>("input_path", "Path to parquet input");
  clip.add_required<std::string>("col_node", "Node label column name");
  clip.add_optional<bool>(
    "add_new", "Add unknown labels as disconnected nodes (default false)",
    false);
  clip.add_optional<std::vector<std::string>>(
    "metadata", "Column names of node metadata fields to ingest", {});

  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path = clip.get_state<std::string>("path");
  auto loglevel = clip.get_state<int>(log_state_name);
  comm.set_logger_target(ygm::logger_target::stderr);
  comm.set_log_level(metalldata::loglevel_py2ygm(loglevel));

  auto input_path = clip.get<std::string>("input_path");
  auto col_node = clip.get<std::string>("col_node");
  auto add_new = clip.get<bool>("add_new");
  auto meta_str = clip.get<std::vector<std::string>>("metadata");

  std::vector<metalldata::metall_graph::series_name> meta;
  meta.reserve(meta_str.size());
  for (const auto& name : meta_str) {
    meta.emplace_back("node", name);
  }

  metalldata::metall_graph graph(comm, path, false);
  auto result = clip.has_argument("metadata")
                  ? graph.ingest_parquet_nodes(input_path, true, col_node,
                                               add_new, meta)
                  : graph.ingest_parquet_nodes(input_path, true, col_node,
                                               add_new);
  if (!result) {
    comm.cerr0(result.error());
    return -1;
  }

  for (const auto& [warning, count] : result.warnings()) {
    comm.cerr0(std::format("{} : {}", warning, count));
  }
  clip.update_selectors(graph.get_selector_info());
  clip.to_return(result.value());
  return 0;
} catch (const std::runtime_error& e) {
  std::cerr << "Error in execution: " << e.what() << "; aborting.\n";
} catch (...) {
  std::cerr << "Unknown error in execution; aborting.\n";
}
