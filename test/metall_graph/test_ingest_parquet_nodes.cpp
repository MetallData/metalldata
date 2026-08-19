// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <cassert>
#include <filesystem>
#include <string>
#include <vector>

#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>

using series_name = metalldata::metall_graph::series_name;

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  const std::filesystem::path data_path = CMAKE_DATA_PATH;
  const auto edge_path = data_path / "metall_graph/pq/path_graph_0.parquet";
  const auto new_node_path =
    data_path / "metall_graph/pq/intsasstrings_0.parquet";
  const std::string metall_path = "ingestnodes";

  if (world.layout().local_id() == 0) {
    std::filesystem::remove_all(metall_path);
  }
  world.barrier();

  {
    metalldata::metall_graph graph(world, metall_path);
    auto edge_result = graph.ingest_parquet_edges(
      edge_path.string(), false, "s", "t", true, std::vector<series_name>{});
    assert(edge_result);
    assert(graph.num_nodes({}) == 7);

    auto existing_result = graph.ingest_parquet_nodes(
      edge_path.string(), false, "s", false,
      std::vector<series_name>{series_name{"node.name"}});
    assert(existing_result);
    assert(existing_result->at("num_nodes_ingested") == 6);
    assert(existing_result->at("num_new_nodes_ingested") == 0);
    assert(graph.num_nodes({}) == 7);
    assert(graph.has_series(series_name{"node.name"}));

    auto skip_result = graph.ingest_parquet_nodes(
      new_node_path.string(), false, "u", false,
      std::vector<series_name>{series_name{"node.time_seen"},
                               series_name{"node.valid"}});
    assert(skip_result);
    assert(skip_result->at("num_nodes_ingested") == 0);
    assert(skip_result->at("num_new_nodes_ingested") == 0);
    assert(graph.num_nodes({}) == 7);
    assert(!skip_result.warnings().empty());

    auto add_result = graph.ingest_parquet_nodes(
      new_node_path.string(), false, "u", true,
      std::vector<series_name>{series_name{"node.time_seen"},
                               series_name{"node.valid"}});
    assert(add_result);
    assert(add_result->at("num_nodes_ingested") == 4);
    assert(add_result->at("num_new_nodes_ingested") == 4);
    assert(graph.num_nodes({}) == 11);
    assert(graph.num_edges({}) == 6);
    assert(graph.has_series(series_name{"node.time_seen"}));
    assert(graph.has_series(series_name{"node.valid"}));

    auto selected = graph.select_nodes(
      {series_name{"node.id"}, series_name{"node.time_seen"},
       series_name{"node.valid"}},
      0, {});
    assert(selected);
    size_t local_metadata_rows = 0;
    selected->for_all([&](const std::vector<metalldata::metall_graph::data_types>&
                            row) {
      if (!std::holds_alternative<std::monostate>(row[1])) {
        assert(std::holds_alternative<std::string>(row[0]));
        assert(std::holds_alternative<std::string>(row[1]));
        assert(std::holds_alternative<bool>(row[2]));
        ++local_metadata_rows;
      }
    });
    assert(ygm::sum(local_metadata_rows, world) == 4);
  }

  if (world.layout().local_id() == 0) {
    std::filesystem::remove_all(metall_path);
  }
  world.barrier();
  return 0;
}
