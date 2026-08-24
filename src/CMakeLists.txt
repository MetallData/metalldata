# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

add_subdirectory(libmetalldata)
if(NOT BUILD_ONLY_LIBMETALLDATA)
  add_subdirectory(clippy)
  add_subdirectory(cli_utils)
endif()