# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

set(CMAKE_BUILD_TYPE Debug)

# include_directories(${Boost_INCLUDE_DIRS})

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

add_metalldata_executable(mf-eval mf-eval.cpp)
setup_metall_target(mf-eval)
setup_ygm_target(mf-eval)
setup_clippy_target(mf-eval)

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

