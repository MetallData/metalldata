// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// dump_parquet_nodes(metadata: []string, where)

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metall_graph.hpp>
#include <format>

static const std::string method_name    = "dump_parquet_nodes";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Writes a parquet file of edge data"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("output_path", "Path to parquet output");
  clip.add_optional<std::vector<std::string>>(
    "metadata", "Column names of additional fields to ingest", {});

  clip.add_optional<bool>(
    "overwrite",
    "If true, overwrite the output file if it exists (default false)", false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path        = clip.get_state<std::string>("path");
  auto output_path = clip.get<std::string>("output_path");
  auto overwrite   = clip.get<bool>("overwrite");
  auto meta_str    = clip.get<std::vector<std::string>>("metadata");

  std::vector<metalldata::metall_graph::series_name> meta;
  meta.reserve(meta_str.size());
  for (const auto& s : meta_str) {
    meta.emplace_back(s);
  }

  metalldata::metall_graph mg(comm, path, false);

  auto result = mg.dump_parquet_verts(output_path, meta, overwrite);

  if (!result.good()) {
    comm.cerr0() << "Error: " << result.error << std::endl;
    return 1;
  }

  for (const auto& [msg, count] : result.warnings) {
    comm.cerr0() << "Warning: " << msg << " (occurred " << count << " times)"
                 << std::endl;
  }

  clip.to_return(0);
  return 0;
}