# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

function(add_metallgraph_executable2 name source)
  add_executable(${name} ${source})
  add_common_compile_options(${name})
  target_link_libraries(${name} PRIVATE libmetalldata)
  target_compile_definitions(${name} PRIVATE METALLDATA_USE_PARQUET)
  target_sources(${name} PRIVATE utils.cpp utils.hpp)
  target_include_directories(${name} PUBLIC ${PROJECT_SOURCE_DIR}/include)
  setup_clippy_target(${name})
  setup_ygm_target(${name})
  setup_metall_target(${name})
  target_link_libraries(${name} PUBLIC Boost::json Boost::container Boost::unordered Boost::interprocess Boost::property_tree Boost::uuid Boost::graph Boost::program_options jsonlogic parquet_writer_lib faker-cxx)
endfunction()


add_subdirectory(MetallGraph)
add_subdirectory(MetallUtils)