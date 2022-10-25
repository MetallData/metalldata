# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-Wall -Wextra -pedantic -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-Wall -Wextra -O3 -march=native")

# include_directories(${Boost_INCLUDE_DIRS})

include_directories(${PROJECT_SOURCE_DIR}/src/MetallFrame/)

add_metalldata_executable(mf-init mf-init.cpp)
setup_metall_target(mf-init)
setup_ygm_target(mf-init)
setup_clippy_target(mf-init)

add_metalldata_executable(mf-read_json mf-read_json.cpp)
setup_metall_target(mf-read_json)
setup_ygm_target(mf-read_json)
setup_clippy_target(mf-read_json)

add_metalldata_executable(mf-getitem mf-getitem.cpp)
setup_metall_target(mf-getitem)
setup_ygm_target(mf-getitem)
setup_clippy_target(mf-getitem)

add_metalldata_executable(mf-count mf-count.cpp)
setup_metall_target(mf-count)
setup_ygm_target(mf-count)
setup_clippy_target(mf-count)

add_metalldata_executable(mf-head mf-head.cpp)
setup_metall_target(mf-head)
setup_ygm_target(mf-head)
setup_clippy_target(mf-head)

add_metalldata_executable(mf-set mf-set.cpp)
setup_metall_target(mf-set)
setup_ygm_target(mf-set)
setup_clippy_target(mf-set)

add_metalldata_executable(mf-merge mf-merge.cpp)
setup_metall_target(mf-merge)
setup_ygm_target(mf-merge)
setup_clippy_target(mf-merge)

add_metalldata_executable(mf-clear mf-clear.cpp)
setup_metall_target(mf-clear)
setup_ygm_target(mf-clear)
setup_clippy_target(mf-clear)


add_metalldata_executable(mf-info mf-info.cpp)
setup_metall_target(mf-info)
setup_ygm_target(mf-info)
setup_clippy_target(mf-info)
