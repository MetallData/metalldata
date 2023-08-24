// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <cstdlib>

namespace json_bento::jbdtl {
namespace value_tag {
using type                               = uint8_t;
static constexpr type        null_tag    = 0;
static constexpr type        bool_tag    = 1;
static constexpr type        int64_tag   = 2;
static constexpr type        uint64_tag  = 3;
static constexpr type        double_tag  = 4;
static constexpr type        string_tag  = 5;
static constexpr type        array_tag   = 6;
static constexpr type        object_tag  = 7;
static constexpr type        tag_list[8] = {null_tag,   bool_tag,   int64_tag,
                                            uint64_tag, double_tag, string_tag,
                                            array_tag,  object_tag};
static constexpr std::size_t num_tags    = sizeof(tag_list);
static constexpr type        max_tag     = *std::max_element(
    value_tag::tag_list, value_tag::tag_list + sizeof(value_tag::tag_list));
}  // namespace value_tag
}  // namespace json_bento::jbdtl
