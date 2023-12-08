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

include_directories(${PROJECT_SOURCE_DIR}/src/MetallFrame/)

add_metalldata_executable(mf-init mf-init.cpp)
setup_metall_target(mf-init)
setup_ygm_target(mf-init)
setup_clippy_target(mf-init)

add_metalldata_executable(mf-read_csv mf-read_csv.cpp)
setup_metall_target(mf-read_csv)
setup_ygm_target(mf-read_csv)
setup_clippy_target(mf-read_csv)

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
