#include <metall_graph.hpp>
#include <ygm/comm.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  std::vector<std::string> cols{"from", "to", "conn_id", "score", "age"};
  metalldata::metall_graph test(world, "ingestedges");
  world.cerr0("past creation of testgraph\n");
  test.ingest_parquet_edges("mgraph1_0.parquet", false, "from", "to", true,
                            cols);
}