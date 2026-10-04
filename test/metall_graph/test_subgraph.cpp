// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <algorithm>
#include <filesystem>
#include <metalldata/metall_graph.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/utility/assert.hpp>

using metall_graph = metalldata::metall_graph;
using series_name = metall_graph::series_name;
using where_clause = metall_graph::where_clause;

// Get the path to the data directory from CMake
std::filesystem::path data_path = CMAKE_DATA_PATH;

static where_clause jl(std::string_view rule) {
  return where_clause(boost::json::parse(rule));
}

static bool contains(const std::vector<series_name>& names,
                     std::string_view                name) {
  return std::ranges::any_of(names,
                             [&](const auto& n) { return n == name; });
}

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  std::filesystem::path parquet_path = data_path / "metall_graph/test";
  std::string           metall_path = "subgraph";

  if (comm.layout().local_id() == 0) {
    // Only one rank per node needs to call remove_all
    std::filesystem::remove_all(metall_path);
  }
  comm.barrier();
  metall_graph mg(comm, metall_path);

  auto ret_ingest =
    mg.ingest_parquet_edges(parquet_path.string(), false, "s", "t", true);
  YGM_ASSERT_RELEASE(ret_ingest);
  YGM_ASSERT_RELEASE(mg.assign(series_name("node.all"), true, {}));

  const auto edge_where = jl(R"({"==":[{"var":"edge.graphnum"},3]})");
  const auto node_where = jl(R"({"==":[{"var":"node.all"},true]})");

  //
  // create from empty, edge and node where clauses
  YGM_ASSERT_RELEASE(mg.create_subgraph("everything", {}));
  YGM_ASSERT_RELEASE(mg.create_subgraph("g3", edge_where));
  YGM_ASSERT_RELEASE(mg.create_subgraph("allnodes", node_where));

  const auto sg_everything = jl(R"({"var":"subgraph.everything"})");
  const auto sg_g3 = jl(R"({"var":"subgraph.g3"})");
  const auto sg_allnodes = jl(R"({"var":"subgraph.allnodes"})");
  YGM_ASSERT_RELEASE(sg_g3.is_subgraph_clause());
  YGM_ASSERT_RELEASE(!sg_g3.is_node_clause() && !sg_g3.is_edge_clause());

  YGM_ASSERT_RELEASE(mg.num_edges(edge_where) > 0);
  YGM_ASSERT_RELEASE(mg.num_edges(edge_where) < mg.num_edges({}));
  YGM_ASSERT_RELEASE(mg.num_nodes(sg_everything) == mg.num_nodes({}));
  YGM_ASSERT_RELEASE(mg.num_edges(sg_everything) == mg.num_edges({}));
  YGM_ASSERT_RELEASE(mg.num_nodes(sg_g3) == mg.num_nodes(edge_where));
  YGM_ASSERT_RELEASE(mg.num_edges(sg_g3) == mg.num_edges(edge_where));
  YGM_ASSERT_RELEASE(mg.num_nodes(sg_allnodes) == mg.num_nodes(node_where));
  YGM_ASSERT_RELEASE(mg.num_edges(sg_allnodes) == mg.num_edges(node_where));

  auto stats = mg.describe(sg_g3);
  YGM_ASSERT_RELEASE(stats);
  const size_t g3_nv = mg.num_nodes(edge_where);
  const size_t g3_ne = mg.num_edges(edge_where);
  if (comm.rank0()) {  // describe reports the global stats on rank 0
    YGM_ASSERT_RELEASE(stats->nv == g3_nv);
    YGM_ASSERT_RELEASE(stats->ne == g3_ne);
  }

  //
  // create from another subgraph
  YGM_ASSERT_RELEASE(mg.create_subgraph("g3copy", sg_g3));
  const auto sg_g3copy = jl(R"({"var":"subgraph.g3copy"})");
  YGM_ASSERT_RELEASE(mg.num_nodes(sg_g3copy) == mg.num_nodes(sg_g3));
  YGM_ASSERT_RELEASE(mg.num_edges(sg_g3copy) == mg.num_edges(sg_g3));

  //
  // hidden series are not listed; subgraphs are
  for (const auto& n : mg.get_node_series_names()) {
    YGM_ASSERT_RELEASE(!n.is_hidden());
  }
  for (const auto& n : mg.get_edge_series_names()) {
    YGM_ASSERT_RELEASE(!n.is_hidden());
  }
  auto sels = mg.get_selector_info();
  for (const auto& [sel, doc] : sels) {
    YGM_ASSERT_RELEASE(sel.find("._") == std::string::npos);
  }
  YGM_ASSERT_RELEASE(sels.contains("subgraph.g3"));
  YGM_ASSERT_RELEASE(sels.contains("node.all"));
  YGM_ASSERT_RELEASE(mg.get_subgraph_names().size() == 4);
  YGM_ASSERT_RELEASE(contains(mg.get_subgraph_names(), "subgraph.g3"));
  YGM_ASSERT_RELEASE(mg.has_series(series_name("subgraph.g3")));
  YGM_ASSERT_RELEASE(!mg.has_series(series_name("subgraph.nope")));

  //
  // a visible series may share a subgraph's name
  YGM_ASSERT_RELEASE(mg.assign(series_name("node.g3"), true, {}));
  YGM_ASSERT_RELEASE(mg.drop_series(series_name("node.g3")));
  YGM_ASSERT_RELEASE(mg.has_series(series_name("subgraph.g3")));

  //
  // invalid creates
  YGM_ASSERT_RELEASE(!mg.create_subgraph("g3", {}));
  YGM_ASSERT_RELEASE(!mg.create_subgraph("", {}));
  YGM_ASSERT_RELEASE(!mg.create_subgraph("_g4", {}));
  YGM_ASSERT_RELEASE(!mg.create_subgraph("a.b", {}));
  YGM_ASSERT_RELEASE(!mg.create_subgraph(
    "mixed",
    jl(R"({"and":[{"var":"subgraph.g3"},{"var":"edge.directed"}]})")));
  YGM_ASSERT_RELEASE(
    !mg.create_subgraph("missing", jl(R"({"var":"subgraph.nope"})")));
  YGM_ASSERT_RELEASE(!mg.has_series(series_name("subgraph.mixed")));
  YGM_ASSERT_RELEASE(!mg.has_series(series_name("subgraph.missing")));

  //
  // a where clause on a missing subgraph is an error, not "everything"
  bool threw = false;
  try {
    mg.num_edges(jl(R"({"var":"subgraph.nope"})"));
  } catch (const std::runtime_error&) {
    threw = true;
  }
  YGM_ASSERT_RELEASE(threw);

  //
  // users cannot touch hidden series
  const series_name hidden_node("node._subgraph_g3");
  const series_name hidden_edge("edge._subgraph_g3");
  YGM_ASSERT_RELEASE(mg.has_series(hidden_node));
  YGM_ASSERT_RELEASE(mg.has_series(hidden_edge));
  YGM_ASSERT_RELEASE(!mg.assign(series_name("node._x"), true, {}));
  YGM_ASSERT_RELEASE(!mg.assign(series_name("edge._x"), true, {}));
  YGM_ASSERT_RELEASE(!mg.add_series<bool>(series_name("node._x")));
  YGM_ASSERT_RELEASE(
    !mg.rename_series(series_name("node.all"), series_name("node._all")));
  YGM_ASSERT_RELEASE(!mg.rename_series(hidden_node, series_name("node.g3")));
  YGM_ASSERT_RELEASE(!mg.drop_series(hidden_node));
  YGM_ASSERT_RELEASE(!mg.in_degree(series_name("node._deg"), {}));
  YGM_ASSERT_RELEASE(
    !mg.connected_components(series_name("node._cc"), {}));
  YGM_ASSERT_RELEASE(!mg.sample_edges(series_name("edge._s"), 1, 0, {}));
  YGM_ASSERT_RELEASE(!mg.sample_nodes(series_name("node._s"), 1, 0, {}));
  YGM_ASSERT_RELEASE(mg.has_series(hidden_node));

  //
  // drop_series on a subgraph removes both hidden series
  YGM_ASSERT_RELEASE(mg.drop_series(series_name("subgraph.g3")));
  YGM_ASSERT_RELEASE(!mg.has_series(series_name("subgraph.g3")));
  YGM_ASSERT_RELEASE(!mg.has_series(hidden_node));
  YGM_ASSERT_RELEASE(!mg.has_series(hidden_edge));
  YGM_ASSERT_RELEASE(!mg.drop_series(series_name("subgraph.g3")));
  YGM_ASSERT_RELEASE(!mg.get_selector_info().contains("subgraph.g3"));
  YGM_ASSERT_RELEASE(mg.get_selector_info().contains("subgraph.g3copy"));
  YGM_ASSERT_RELEASE(mg.get_subgraph_names().size() == 3);

  comm.cout0("test_subgraph passed");
  return 0;
}
