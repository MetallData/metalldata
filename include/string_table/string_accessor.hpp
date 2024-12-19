// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <cassert>
#include <cstddef>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace compact_string {

/// \brief Provides a way to access a string stored in a string store.
/// If a string is short, it stores the string in the object itself.
/// If a string is long, it stores the pointer to the string in the object.
/// Can take only strings allocated by allocate_string_embedding_length(),
/// however, w/o the length prefix.
class string_accessor {
public:
  using size_type = std::size_t;
  using char_type = char;
  using offset_t = std::ptrdiff_t;

private:
  using self_type = string_accessor;

  static constexpr size_t k_num_blocks = sizeof(offset_t);
  static constexpr size_t k_short_str_max_length =
      k_num_blocks - 2; // -1 for '\0' and -1 for metadata

public:
  string_accessor() = default;

  /// \brief Construct a string accessor from a pointer to string.
  /// \param data A pointer to the string data. This must be a pointer to a
  /// actual string data, not the address that points to the length data.
  explicit string_accessor(const char_type *data) { assign(data); }

  string_accessor(const char_type *data, size_type length) {
    assign(data, length);
  }

  string_accessor(const string_accessor &other) {
    if (other.is_short()) {
      m_entier_block = other.m_entier_block;
    } else {
      // Memo: we cannot copy the pointer directly
      // as offset must be recalculated.
      priv_set_long_str_pointer(other.priv_to_long_str_pointer());
    }
  }

  string_accessor(string_accessor &&other) {
    if (other.is_short()) {
      m_entier_block = other.m_entier_block;
    } else {
      // Memo: we cannot copy the pointer directly
      // as offset must be recalculated.
      priv_set_long_str_pointer(other.priv_to_long_str_pointer());
    }
    other.m_entier_block = 0; // clear the data
  }

  string_accessor &operator=(const string_accessor &other) {
    if (this == &other) {
      return *this;
    }
    if (other.is_short()) {
      m_entier_block = other.m_entier_block;
    } else {
      priv_set_long_str_pointer(other.priv_to_long_str_pointer());
    }
    return *this;
  }

  string_accessor &operator=(string_accessor &&other) noexcept {
    if (other.is_short()) {
      m_entier_block = other.m_entier_block;
    } else {
      priv_set_long_str_pointer(other.priv_to_long_str_pointer());
    }
    other.m_entier_block = 0; // clear the data
    return *this;
  }

  ~string_accessor() noexcept = default;

  static constexpr size_t short_str_max_length() {
    return k_short_str_max_length;
  }

  bool is_short() const { return !is_long(); }

  bool is_long() const { return priv_get_long_flag(); }

  size_type length() const {
    if (is_short()) {
      return priv_get_short_length();
    }
    /// This depends on the internal implementation of the string store
    auto *ptr = reinterpret_cast<size_t *>(priv_to_long_str_pointer());
    return *(ptr - 1);
  }

  const char_type *c_str() const {
    if (is_short()) {
      return priv_get_short_str();
    }
    return priv_to_long_str_pointer();
  }

  std::string_view to_view() const {
    return std::string_view{c_str(), length()};
  }

  void assign(const char_type *data) {
    assign(data, std::char_traits<char_type>::length(data));
  }

  void assign(const char_type *data, size_type length) {
    if (length <= k_short_str_max_length) {
      priv_set_short_str(data, length);
    } else {
      priv_set_long_str_pointer(data);
    }
  }

private:
  bool priv_get_long_flag() const { return m_blocks[k_num_blocks - 1] & 0x1; }

  void priv_set_long_str_pointer(const char_type *const str) {
    std::ptrdiff_t off = reinterpret_cast<std::ptrdiff_t>(str) -
                         reinterpret_cast<std::ptrdiff_t>(this);

    bool is_negative = false;
    if (off < 0) {
      off = -off;
      is_negative = true;
    }
    if (uint64_t(off) > (1ULL << 55)) {
      std::cerr << "Fatal error: offset is too large" << std::endl;
      std::abort();
    }

    // Manually set the offset to the block array
    // This should be endian-safe
    for (uint i = 0; i < k_num_blocks - 1; ++i) {
      m_blocks[i] = char(off & 0xFFULL);
      off >>= 8ULL;
    }

    // Finally set the metadata
    char metadata = 0;
    if (is_negative) {
      metadata |= 0x2; // set the negative bit
    }
    metadata |= 0x1; // set the long string bit

    m_blocks[k_num_blocks - 1] = metadata;
  }

  char_type *priv_to_long_str_pointer() const {
    assert(is_long());
    std::ptrdiff_t off = 0;
    for (int i = int(k_num_blocks) - 2; i >= 0; --i) {
      off <<= 8;
      off |= m_blocks[i];
    }
    if (m_blocks[k_num_blocks - 1] & 0x2) {
      off = -off;
    }
    auto addr =
        reinterpret_cast<std::ptrdiff_t>(const_cast<self_type *>(this)) + off;

    return reinterpret_cast<char_type *>(addr);
  }

  void priv_set_short_str(const char_type *const str, size_type length) {
    assert(length <= k_short_str_max_length);
    m_entier_block = 0;

    for (int i = 0; i < int(length); ++i) {
      m_blocks[i] = str[i];
    }
    m_blocks[length] = '\0';
    // Set the length and the short string flag
    m_blocks[k_num_blocks - 1] = char((length << 1) & 0xFE);
  }

  size_type priv_get_short_length() const {
    assert(is_short());
    return (m_blocks[k_num_blocks - 1] >> 1) & 0x7;
  }

  const char_type *priv_get_short_str() const {
    assert(is_short());
    return m_blocks;
  }

  // 1 bit for short/long flag
  // for short string,
  //    the next 7 bits for length
  //    the rest 56 bits for string (6 characters + '\0')
  // for long string,
  //    the last 56 bits for pointer (48 bits should be enough though)
  union {
    char m_blocks[sizeof(offset_t)] = {0};
    uint64_t m_entier_block;
    static_assert(sizeof(offset_t) == sizeof(uint64_t),
                  "sizeof(offset_ptr_t) != sizeof(uint64_t)");
  };
};
} // namespace compact_string
