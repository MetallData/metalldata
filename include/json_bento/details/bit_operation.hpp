// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdlib>

namespace json_bento::jbdtl {
static constexpr uint64_t get_lsb(const uint64_t value) {
  uint64_t lsb = 0;
  auto     n   = value;
  while (!(n & 0x1ULL)) {
    ++lsb;
    n >>= 1;
  }
  return lsb;
}
}  // namespace json_bento::jbdtl
