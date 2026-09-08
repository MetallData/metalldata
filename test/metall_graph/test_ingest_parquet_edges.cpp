// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <map>
#include <metalldata/metall_graph.hpp>
#include <string>
#include <variant>
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/utility/assert.hpp>

using series_name = metalldata::metall_graph::series_name;

// Get the path to the data directory from CMake
std::filesystem::path data_path = CMAKE_DATA_PATH;

namespace metalldata {
class metall_graph_test {
 public:
  void run_test(ygm::comm& comm) {
    std::filesystem::path    parquet_path = data_path / "metall_graph/pqmulti";
    std::string              metall_path = "ingestedges";  // Default path
    std::vector<series_name> cols{series_name("edge.color"),
                                  series_name("edge.name"),
                                  series_name("edge.weight")};
    std::map<series_name, metalldata::metall_graph::data_types> tags{
      {series_name("edge.source"), std::string("pqmulti")},
      {series_name("edge.batch"), int64_t{7}},
      {series_name("edge.reviewed"), true},
      {series_name("edge.confidence"), 0.75},
      {series_name("edge.weight"), int64_t{100}}};

    if (comm.layout().local_id() == 0) {
      // Only one rank per node needs to call remove_all
      std::filesystem::remove_all(metall_path);
    }
    comm.barrier();
    metalldata::metall_graph test(comm, metall_path);
    comm.cerr0("past creation of testgraph\n");
    auto ret_ingest = test.ingest_parquet_edges(
      parquet_path.string(), false, "s", "t", true, cols, tags);
    if (!ret_ingest) {
      comm.cout(ret_ingest.error());
      MPI_Abort(comm.get_mpi_comm(), 1);
    }
    YGM_ASSERT_RELEASE(ret_ingest->at("num_edges_ingested") > 0);
    YGM_ASSERT_RELEASE(ret_ingest->at("num_new_nodes_ingested") > 0);
    YGM_ASSERT_RELEASE(
      ret_ingest.warnings().contains(
        "duplicate or invalid tag name: edge.weight"));

    auto tagged_edges = test.select_edges(
      {series_name("edge.source"), series_name("edge.batch"),
       series_name("edge.reviewed"), series_name("edge.confidence")},
      0, {});
    YGM_ASSERT_RELEASE(tagged_edges);

    size_t local_tagged_edges = 0;
    tagged_edges->for_all(
      [&](const std::vector<metalldata::metall_graph::data_types>& edge) {
        YGM_ASSERT_RELEASE(std::get<std::string>(edge[0]) == "pqmulti");
        YGM_ASSERT_RELEASE(std::get<int64_t>(edge[1]) == 7);
        YGM_ASSERT_RELEASE(std::get<bool>(edge[2]));
        YGM_ASSERT_RELEASE(std::get<double>(edge[3]) == 0.75);
        ++local_tagged_edges;
      });
    YGM_ASSERT_RELEASE(ygm::sum(local_tagged_edges, comm) ==
                       ret_ingest->at("num_edges_ingested"));

    auto ret_check = test.priv_check_index_integrity();
    if (!ret_check) {
      comm.cout(ret_check.error());
      MPI_Abort(comm.get_mpi_comm(), 1);
    }
  }
};
}  // namespace metalldata

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  metalldata::metall_graph_test mgt;

  mgt.run_test(world);
  return 0;
}
