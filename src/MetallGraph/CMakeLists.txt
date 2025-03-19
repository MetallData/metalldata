# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

#set(CMAKE_CXX_FLAGS_DEBUG "-Wall -Wextra -pedantic -Wno-unused-parameter -O0 -g")
#set(CMAKE_CXX_FLAGS_RELEASE "-Wall -Wextra -O3 -march=native")

# include_directories(${Boost_INCLUDE_DIRS})

include_directories(${PROJECT_SOURCE_DIR}/src/MetallJsonLines/)
include_directories(${PROJECT_SOURCE_DIR}/src/MetallGraph/)

add_metalldata_executable(__init__ __init__.cpp)
setup_clippy_target(__init__)
setup_ygm_target(__init__)
setup_metall_target(__init__)

add_metalldata_executable(read_edges read_edges.cpp)
setup_clippy_target(read_edges)
setup_ygm_target(read_edges)
setup_metall_target(read_edges)