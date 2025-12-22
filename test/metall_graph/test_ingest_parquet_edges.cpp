#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>
#include <vector>
#include <string>

using series_name = metalldata::metall_graph::series_name;

int main(int argc, char** argv) {
  std::string parquet_path = "multiparq";    // Default path
  std::string metall_path  = "ingestedges";  // Default path
  if (argc > 1) {
    parquet_path = argv[1];
  }
  if (argc > 2) {
    metall_path = argv[2];
  }

  ygm::comm world(&argc, &argv);

  std::vector<series_name> cols{series_name("edge.conn_id"),
                                series_name("edge.score"),
                                series_name("edge.age")};
  metalldata::metall_graph test(world, metall_path);
  world.cerr0("past creation of testgraph\n");
  test.ingest_parquet_edges(parquet_path, false, "from", "to", true, cols);
}