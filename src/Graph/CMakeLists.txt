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

include_directories(${PROJECT_SOURCE_DIR}/src/Graph/)

add_metalldata_executable(__init__ __init__.cpp)
setup_metall_target(__init__)
setup_ygm_target(__init__)
setup_clippy_target(__init__)

add_metalldata_executable(size size.cpp)
setup_metall_target(size)
setup_ygm_target(size)
setup_clippy_target(size)

add_metalldata_executable(add add.cpp)
setup_metall_target(add)
setup_ygm_target(add)
setup_clippy_target(add)

add_custom_command(
        TARGET size POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/meta.json
                ${CMAKE_CURRENT_BINARY_DIR}/meta.json)