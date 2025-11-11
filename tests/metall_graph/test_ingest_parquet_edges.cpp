#include <metall_graph.hpp>
#include <ygm/comm.hpp>
#include <vector>
#include <string>

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

  std::vector<std::string> cols{"conn_id", "score", "age"};
  metalldata::metall_graph test(world, metall_path);
  world.cerr0("past creation of testgraph\n");
  test.ingest_parquet_edges(parquet_path, false, "from", "to", true, cols);
}