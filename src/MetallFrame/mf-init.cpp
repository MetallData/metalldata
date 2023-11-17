// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements the construction of a MetallFrame object.

//~ #include "mf-common.hpp"

#include "clippy/clippy.hpp"
#include "mf-common.hpp"

namespace xpr = experimental;



using ColumnDescription = std::pair<std::string, std::string>;

const std::string& type(const ColumnDescription& col) { return col.second; }
const std::string& name(const ColumnDescription& col) { return col.first; }

namespace {
const std::string METHOD_NAME     = "__init__";
const std::string METHOD_DESC =
    "Initializes a MetallFrame object\n"
    "creates a new physical object on disk "
    "only if it does not already exist.";

const std::string ARG_COLUMN_NAME = "columns";
const std::string ARG_COLUMN_DESC =
  "Column description (pair of string/string describing name and type of "
  "columns)."
  "\n  Valid types in (string | int | uint | real)"
  "\n  When the column description is supplied, any existing dataframe"
  "\n  at the specified location will be overwritten";
}  // namespace

void append_column(xpr::metall_frame& mf, const ColumnDescription& desc) {
  static const std::string UNKNOWN_COLUMN_TYPE{"unknown column type: "};

  std::string_view colname = name(desc);

  if (type(desc) == "uint")
    mf.add_column_with_default(colname, xpr::dense<xpr::uint_t>{0});
  else if (type(desc) == "int")
    mf.add_column_with_default(colname, xpr::dense<xpr::int_t>{0});
  else if (type(desc) == "real")
    mf.add_column_with_default(colname, xpr::dense<xpr::real_t>{0});
  else if (type(desc) == "string")
    mf.add_column_with_default(
        colname,
        xpr::dense<xpr::string_t>{mf.persistent_string("")});
  else
    throw std::runtime_error{UNKNOWN_COLUMN_TYPE + name(desc)};
}

void append_columns(xpr::metall_frame& mf,
                    const std::vector<ColumnDescription>& cols) {
  for (const ColumnDescription& desc : cols) append_column(mf, desc);
}

int ygm_main(ygm::comm& world, int argc, char** argv) {
  int            error_code = 0;
  clippy::clippy clip{METHOD_NAME, METHOD_DESC};

  clip.member_of(MF_CLASS_NAME, "A " + MF_CLASS_NAME + " class");
  clip.add_required<std::string>(ST_METALL_LOCATION_NAME,
                                 ST_METALL_LOCATION_DESC);
  clip.add_optional<std::string>(ST_METALLFRAME_NAME,
                                 ST_METALLFRAME_DESC,
                                 ST_METALLFRAME_DFLT);

  // ARG_COLUMN_DESC is optional. If not present, the __init__ method assumes
  // that we want to reopen an existing dataframe.
  clip.add_optional<std::vector<ColumnDescription>>(
     ARG_COLUMN_NAME, ARG_COLUMN_DESC,std::vector<ColumnDescription>{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv)) {
    return 0;
  }

  // the real thing
  try {
    using metall_manager = xpr::metall_frame::metall_manager_type;

    // try to create the object
    std::string dataLocation = clip.get<std::string>(ST_METALL_LOCATION_NAME);

    if (!metall::utility::metall_mpi_adaptor::consistent(dataLocation.data(),
                                                         MPI_COMM_WORLD))
      throw std::runtime_error{"Metallstore is inconsistent"};

    std::string key          = clip.get<std::string>(ST_METALLFRAME_NAME);
    std::vector<ColumnDescription> column_desc =
          clip.get<std::vector<ColumnDescription>>(ARG_COLUMN_DESC);

    const bool  createNew    = column_desc.size() != 0;

    if (createNew) {
      metall_manager mm{metall::create_only, dataLocation.data(),
                        MPI_COMM_WORLD};

      xpr::metall_frame::create_new(mm, world, key);
      xpr::metall_frame  frame{mm, world, key};

      append_columns(frame, column_desc);
    } else {
      metall_manager mm{metall::open_read_only, dataLocation.data(),
                        MPI_COMM_WORLD};

      // check that storage is in consistent state
      xpr::metall_frame::check_state(mm, world, key);
    }

    // set the return values
    clip.set_state(ST_METALL_LOCATION_NAME, std::move(dataLocation));
    clip.set_state(ST_METALLFRAME_NAME, std::move(key));
  } catch (const std::runtime_error& ex) {
    error_code = 1;
    clip.to_return(ex.what());
  }

  return error_code;
}
