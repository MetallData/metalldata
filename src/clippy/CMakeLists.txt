# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

function(add_metallgraph_executable2 name source)
  add_executable(${name} ${source})
  add_common_compile_options(${name})
  target_link_libraries(${name} PRIVATE libmetalldata)
  target_include_directories(${name} PUBLIC ${PROJECT_SOURCE_DIR}/include)
  setup_clippy_target(${name})
  setup_ygm_target(${name})
  setup_metall_target(${name})
endfunction()


add_subdirectory(MetallGraph)
add_subdirectory(MetallUtils)