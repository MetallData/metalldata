#pragma once

#include <cstdlib>

namespace json_bento::jbdtl {
static constexpr uint64_t get_lsb(const uint64_t value) {
  uint64_t lsb = 0;
  auto n = value;
  while (!(n & 0x1ULL)) {
    ++lsb;
    n >>= 1;
  }
  return lsb;
}
}  // namespace json_bento::jbdtl

