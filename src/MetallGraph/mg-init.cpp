// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements MetallGraph constructor (__init__).

#include "mg-common.hpp"

namespace xpr = experimental;

namespace {
const std::string METHOD_NAME = "__init__";
const std::string METHOD_DOCSTRING =
    "Initializes a MetallGraph object\n"
    "creates a new physical object on disk "
    "only if it does not already exist.";

using ARG_VERTEX_KEY_TYPE             = std::string;
const std::string ARG_VERTEX_KEY_NAME = "key";
const std::string ARG_VERTEX_KEY_DESC =
    "The key field in each json entry. If a Json object does not have a key it "
    "is not stored."
    "\n(note: The key field is only required when a new data store is created)";

using ARG_EDGE_SRCKEY_TYPE             = ARG_VERTEX_KEY_TYPE;
const std::string ARG_EDGE_SRCKEY_NAME = "srckey";
const std::string ARG_EDGE_SRCKEY_DESC =
    "The source key field in each json entry. If a Json object does not have a "
    "key it is not stored."
    "\n(note: The source key field is only required when a new data store is "
    "created)";

using ARG_EDGE_DSTKEY_TYPE             = ARG_EDGE_SRCKEY_TYPE;
const std::string ARG_EDGE_DSTKEY_NAME = "dstkey";
const std::string ARG_EDGE_DSTKEY_DESC =
    "The destination key field in each json entry. If a Json object does not "
    "have a key it is not stored."
    "\n(note: The destination key field is only required when a new data store "
    "is created)";

using ARG_ALWAYS_CREATE_TYPE             = bool;
const std::string ARG_ALWAYS_CREATE_NAME = "overwrite";
const std::string ARG_ALWAYS_CREATE_DESC =
    "create new data store (deleting any existing data)";
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DOCSTRING};

  clip.member_of(MG_CLASS_NAME, "A " + MG_CLASS_NAME + " class");

  clip.add_required<std::string>(ST_METALL_LOCATION,
                                 "Location of the Metall store");

  // \note the keys are only required when a new data store is created
  //       otherwise they will be read from the existing key fields in
  //       metallgraph
  clip.add_optional<ARG_VERTEX_KEY_TYPE>(ARG_VERTEX_KEY_NAME,
                                         ARG_VERTEX_KEY_DESC, std::string{});
  clip.add_optional<ARG_EDGE_SRCKEY_TYPE>(ARG_EDGE_SRCKEY_NAME,
                                          ARG_EDGE_SRCKEY_DESC, std::string{});
  clip.add_optional<ARG_EDGE_DSTKEY_TYPE>(ARG_EDGE_DSTKEY_NAME,
                                          ARG_EDGE_DSTKEY_DESC, std::string{});

  clip.add_optional<bool>(ARG_ALWAYS_CREATE_NAME, ARG_ALWAYS_CREATE_DESC,
                          false);

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    // the real thing
    // try to create the object
    using metall_manager = xpr::metall_json_lines::metall_manager_type;

    const std::string dataLocation = clip.get<std::string>(ST_METALL_LOCATION);
    const ARG_VERTEX_KEY_TYPE vertexKey =
        clip.get<ARG_VERTEX_KEY_TYPE>(ARG_VERTEX_KEY_NAME);
    const ARG_EDGE_SRCKEY_TYPE edgeSrcKey =
        clip.get<ARG_EDGE_SRCKEY_TYPE>(ARG_EDGE_SRCKEY_NAME);
    const ARG_EDGE_DSTKEY_TYPE edgeDstKey =
        clip.get<ARG_EDGE_DSTKEY_TYPE>(ARG_EDGE_DSTKEY_NAME);
    const bool overwrite =
        clip.get<ARG_ALWAYS_CREATE_TYPE>(ARG_ALWAYS_CREATE_NAME);

    if (overwrite) remove_directory_and_content(world, dataLocation);

    if (!std::filesystem::is_directory(dataLocation)) {
      static std::string missingKeyA = "vertex key undefined (set ";
      static std::string missingKeyZ = " )";

      if (vertexKey.size() == 0)
        throw std::runtime_error{missingKeyA + ARG_VERTEX_KEY_NAME +
                                 missingKeyZ};
      if (edgeSrcKey.size() == 0)
        throw std::runtime_error{missingKeyA + ARG_EDGE_SRCKEY_NAME +
                                 missingKeyZ};
      if (edgeDstKey.size() == 0)
        throw std::runtime_error{missingKeyA + ARG_EDGE_DSTKEY_NAME +
                                 missingKeyZ};

      metall_manager mm{metall::create_only, dataLocation.data(),
                        MPI_COMM_WORLD};

      xpr::metall_graph::create_new(mm, world, vertexKey, edgeSrcKey,
                                    edgeDstKey);
    } else {
      metall_manager mm{metall::open_read_only, dataLocation.data(),
                        MPI_COMM_WORLD};

      // check that storage is in consistent state
      xpr::metall_graph::check_state(mm, world);
      // \todo
      //   support following checking variant:
      //   xpr::metall_graph::check_state(mm, world, vertexKey, edgeSrcKey,
      //   edgeDstKey);
    }

    // create the return object
    if (world.rank() == 0) {
      clip.set_state(ST_METALL_LOCATION, std::move(dataLocation));
    }
  } catch (const std::exception& ex) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(ex.what());
  } catch (...) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return("Unknown error");
  }

  return error_code;
}
