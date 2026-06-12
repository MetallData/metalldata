#include <mpi.h>
#include <filesystem>
#include <metalldata/metall_graph.hpp>
#include <ygm/comm.hpp>
#include <vector>
#include <string>
#include <ygm/utility/assert.hpp>

using series_name = metalldata::metall_graph::series_name;

namespace metalldata {
class metall_graph_test {
 public:
  void run_test(ygm::comm& comm) {
    std::string parquet_path =
      "../../../data/metall_graph/pqmulti/";               // Default path
    std::string              metall_path = "ingestedges";  // Default path
    std::vector<series_name> cols{series_name("edge.color"),
                                  series_name("edge.name"),
                                  series_name("edge.weight")};

    if (comm.layout().local_id() == 0) {
      // Only one rank per node needs to call remove_all
      std::filesystem::remove_all(metall_path);
    }
    comm.barrier();
    metalldata::metall_graph test(comm, metall_path);
    comm.cerr0("past creation of testgraph\n");
    auto ret_ingest =
      test.ingest_parquet_edges(parquet_path, false, "s", "t", true, cols);
    if (!ret_ingest) {
      comm.cout(ret_ingest.error());
      MPI_Abort(comm.get_mpi_comm(), 1);
    }

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

  std::filesystem::path cwd = std::filesystem::current_path();

  std::cout << "Current working directory: " << cwd.string() << '\n';

  metalldata::metall_graph_test mgt;

  mgt.run_test(world);
  return 0;
}