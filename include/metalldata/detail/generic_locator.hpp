#pragma once

#include <optional>
#include <utility>
#include <cstddef>

namespace metalldata::detail {
enum class generic_locator : size_t;

constexpr unsigned RANK_BITS = 20;

inline int owner(generic_locator gl) {
  return std::to_underlying(gl) >> (64 - RANK_BITS);
}

inline size_t local(generic_locator gl) {
  return std::to_underlying(gl) & (~size_t{0} >> RANK_BITS);
}

inline std::optional<generic_locator> init_generic_locator(int    owner,
                                                           size_t local) {
  bool bowner = size_t(owner) < ((size_t{1} << RANK_BITS) - 1);
  bool blocal = local < ((size_t{1} << (64 - RANK_BITS)) - 1);
  if (bowner && blocal) {
    size_t to_return = owner;
    to_return <<= (64 - detail::RANK_BITS);
    to_return |= local;
    return generic_locator{to_return};
  }
  return std::nullopt;
}
}  // namespace metalldata::detail