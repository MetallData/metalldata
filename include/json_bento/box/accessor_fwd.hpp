// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <iostream>

namespace json_bento::jbdtl {
template <typename core_data_allocator_type>
class value_accessor;

template <typename core_data_allocator_type>
class array_accessor;

template <typename core_data_allocator_type>
class object_accessor;

template <typename core_data_allocator_type>
class key_value_pair_accessor;

template <typename core_data_allocator_type>
class string_accessor;
}  // namespace json_bento::jbdtl

template <typename storage_allocator_type>
std::ostream &operator<<(
    std::ostream                                                     &os,
    const json_bento::jbdtl::string_accessor<storage_allocator_type> &sa);
