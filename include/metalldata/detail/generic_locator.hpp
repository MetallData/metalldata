#pragma once

// TODO:   Move this file to base YGM later.

#include <optional>
#include <stdexcept>
#include <utility>
#include <cstddef>

namespace metalldata::detail {
enum class generic_locator : size_t;
using rank_type = int;

constexpr unsigned RANK_BITS = 20;

inline rank_type owner(generic_locator gl) {
  return static_cast<rank_type>(std::to_underlying(gl) >> (64 - RANK_BITS));
}

inline size_t local(generic_locator gl) {
  return std::to_underlying(gl) & (~size_t{0} >> RANK_BITS);
}

inline generic_locator init_generic_locator(rank_type owner, size_t local) {
  bool bowner =
    owner >= rank_type{0} && size_t(owner) < ((size_t{1} << RANK_BITS) - 1);
  bool blocal = local < ((size_t{1} << (64 - RANK_BITS)) - 1);
  if (bowner && blocal) {
    size_t to_return = owner;
    to_return <<= (64 - detail::RANK_BITS);
    to_return |= local;
    return generic_locator{to_return};
  }
  throw std::runtime_error(
    "init_generic_locator::owner or local out of bounds for generic locator");
}
}  // namespace metalldata::detail