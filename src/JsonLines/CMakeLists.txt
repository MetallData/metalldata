# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

set(CMAKE_BUILD_TYPE Debug)

# include_directories(${Boost_INCLUDE_DIRS})

add_metalldata_executable(jsonlines-init jsonlines-init.cpp)
setup_metall_target(jsonlines-init)
setup_ygm_target(jsonlines-init)
setup_clippy_target(jsonlines-init)


add_metalldata_executable(jsonlines-importFile jsonlines-importFile.cpp)
setup_metall_target(jsonlines-importFile)
setup_ygm_target(jsonlines-importFile)
setup_clippy_target(jsonlines-importFile)


add_metalldata_executable(jsonlines-getitem jsonlines-getitem.cpp)
setup_metall_target(jsonlines-getitem)
setup_ygm_target(jsonlines-getitem)
setup_clippy_target(jsonlines-getitem)

add_metalldata_executable(jsonlines-eval jsonlines-eval.cpp)
setup_metall_target(jsonlines-eval)
setup_ygm_target(jsonlines-eval)
setup_clippy_target(jsonlines-eval)

