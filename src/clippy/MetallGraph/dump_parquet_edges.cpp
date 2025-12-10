// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// dump_parquet_nodes(metadata: []string, where)

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include <format>

static const std::string method_name    = "dump_parquet_edges";
static const std::string state_name     = "INTERNAL";
static const std::string sel_state_name = "selectors";

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{method_name, "Writes a parquet file of edge data"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("output_path", "Path to parquet output");
  clip.add_optional<std::vector<std::string>>("fields",
                                              "names of series to ingest", {});

  clip.add_optional<bool>(
    "overwrite",
    "If true, overwrite the output file if it exists (default false)", false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  bool all = !clip.has_argument(
    "metadata");  // if we don't specify it at all, include everything

  auto path        = clip.get_state<std::string>("path");
  auto output_path = clip.get<std::string>("output_path");
  auto overwrite   = clip.get<bool>("overwrite");

  metalldata::metall_graph mg(comm, path, false);

  std::vector<metalldata::metall_graph::series_name> meta;
  if (!all) {
    auto meta_str = clip.get<std::vector<std::string>>("metadata");

    meta.reserve(meta_str.size());
    for (const auto& s : meta_str) {
      meta.emplace_back(s);
    }
  } else {
    meta = mg.get_edge_series_names();
  }
  auto result = mg.dump_parquet_edges(output_path, meta, overwrite);

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