# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-Wall -Wextra -pedantic -Wno-unused-parameter -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-Wall -Wextra -O3 -march=native")

# include_directories(${Boost_INCLUDE_DIRS})

include_directories(${PROJECT_SOURCE_DIR}/src/MetallJsonLines/)
include_directories(${PROJECT_SOURCE_DIR}/src/MetallGraph/)

add_metalldata_executable(mg-init mg-init.cpp)
setup_metall_target(mg-init)
setup_ygm_target(mg-init)
setup_clippy_target(mg-init)

#~ add_metalldata_executable(mjl-read_json mjl-read_json.cpp)
#~ setup_metall_target(mjl-read_json)
#~ setup_ygm_target(mjl-read_json)
#~ setup_clippy_target(mjl-read_json)

#~ add_metalldata_executable(mjl-getitem mjl-getitem.cpp)
#~ setup_metall_target(mjl-getitem)
#~ setup_ygm_target(mjl-getitem)
#~ setup_clippy_target(mjl-getitem)

#~ add_metalldata_executable(mjl-count mjl-count.cpp)
#~ setup_metall_target(mjl-count)
#~ setup_ygm_target(mjl-count)
#~ setup_clippy_target(mjl-count)

#~ add_metalldata_executable(mjl-head mjl-head.cpp)
#~ setup_metall_target(mjl-head)
#~ setup_ygm_target(mjl-head)
#~ setup_clippy_target(mjl-head)

#~ add_metalldata_executable(mjl-set mjl-set.cpp)
#~ setup_metall_target(mjl-set)
#~ setup_ygm_target(mjl-set)
#~ setup_clippy_target(mjl-set)

#~ add_metalldata_executable(mjl-merge mjl-merge.cpp)
#~ setup_metall_target(mjl-merge)
#~ setup_ygm_target(mjl-merge)
#~ setup_clippy_target(mjl-merge)

#~ add_metalldata_executable(mjl-clear mjl-clear.cpp)
#~ setup_metall_target(mjl-clear)
#~ setup_ygm_target(mjl-clear)
#~ setup_clippy_target(mjl-clear)


#~ add_metalldata_executable(mjl-info mjl-info.cpp)
#~ setup_metall_target(mjl-info)
#~ setup_ygm_target(mjl-info)
#~ setup_clippy_target(mjl-info)

#~ add_metalldata_executable(rep2 rep2.cpp)
#~ setup_metall_target(rep2)
#~ setup_ygm_target(rep2)
#~ setup_clippy_target(rep2)
