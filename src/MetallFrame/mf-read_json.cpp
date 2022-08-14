// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData Project Developers.
// See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements distributed processing of a json file
///        based on the distributed YGM line parser.

#include "clippy/clippy.hpp"

#include "mf-common.hpp"
#include <ygm/io/csv_parser.hpp>

namespace mtlutil = metall::utility;
namespace mtljsn  = metall::container::experimental::json;

namespace
{
const std::string ARG_IMPORTED = "Json file";
const std::string METHOD_NAME  = "read_json";
} // anonymous

int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int             error_code = 0;
  clippy::clippy  clip{METHOD_NAME, "Imports Json Data from files into the MetallFrame object."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_required<std::vector<std::string> >(ARG_IMPORTED, "Json files to be ingested.");
  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv)) { return 0; }

  try
  {
    std::string                 filename = clip.get<std::string>(ARG_IMPORTED);
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    mtlutil::metall_mpi_adaptor manager(metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD);
    vector_json_type*           vec = &jsonVector(manager);
    ygm::io::line_parser        lineParser{ world, std::vector<std::string>{filename} };
    int                         imported    = 0;
    int                         initialSize = vec->size();
    auto*                       metallmgr = &manager.get_local_manager();

    lineParser.for_all( [&imported, metallmgr, vec](const std::string& line)
                        {
                          vec->emplace_back(mtljsn::parse(line, metallmgr->get_allocator()));
                          ++imported;
                        }
                      );

    assert(int(vec->size()) == initialSize + imported);

    // not necessary here, but common to finish processing all messages
    world.barrier();

    int totalImported = world.all_reduce_sum(imported);

    if (world.rank() == 0)
    {
      std::stringstream msg;

      msg << totalImported << " rows imported" << std::flush;
      clip.to_return(msg.str());
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


