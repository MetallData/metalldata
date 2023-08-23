// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include <metall/offset_ptr.hpp>

namespace json_bento::jbdtl {

template <typename pointer>
class compact_string {
 public:
  using char_type      = char;
  using size_type      = std::size_t;
  using view_type      = std::string_view;
  using iterator       = char_type *;
  using const_iterator = const char_type *;

 private:
  using char_pointer =
      typename std::pointer_traits<pointer>::template rebind<char_type>;

 public:
  compact_string() = default;

  template <typename allocator_type>
  compact_string(const char_type *const s, const size_type count,
                 const allocator_type &alloc) {
    priv_allocate_str(s, count, alloc);
  }

  /// \brief Copy constructor
  compact_string(const compact_string &) = delete;

  /// \brief Allocator-extended copy constructor
  template <typename allocator_type>
  compact_string(const compact_string &other, const allocator_type &alloc) {
    priv_allocate_str(other.c_str(), other.length(), alloc);
  }

  /// \brief Move constructor
  compact_string(compact_string &&other) noexcept {
    if (other.priv_short_key()) {
      m_short_str_buf = std::move(other.m_short_str_buf);
    } else {
      m_long_str = std::move(other.m_long_str);
    }
    m_str_length = other.m_str_length;
    other.priv_clear_variables();
  }

  // Note: Cannot make Allocator-extended move constructor as it needs to check
  // allocator instances.

  /// \brief Copy assignment operator
  compact_string &operator=(const compact_string &other) = delete;

  /// \brief Move assignment operator
  compact_string &operator=(compact_string &&other) noexcept {
    if (other.priv_short_key()) {
      m_short_str_buf = std::move(other.m_short_str_buf);
    } else {
      m_long_str = std::move(other.m_long_str);
    }
    m_str_length = other.m_str_length;
    other.priv_clear_variables();

    return *this;
  }

  void swap(compact_string &other) noexcept {
    using std::swap;
    if (other.priv_short_key()) {
      swap(m_short_str_buf, other.m_short_str_buf);
    } else {
      swap(m_long_str, other.m_long_str);
    }
    swap(m_str_length, other.m_str_length);
  }

  /// \brief Destructor
  ~compact_string() noexcept { assert(priv_short_key() || !m_long_str); }

  /// \brief Returns the stored str.
  /// \return Returns the stored str.
  view_type str_view() const noexcept { return view_type(c_str(), length()); }

  /// \brief Returns the stored str as const char*.
  /// \return Returns the stored str as const char*.
  const char_type *c_str() const noexcept {
    if (priv_short_key()) {
      return m_short_str;
    }
    const auto *const ptr = metall::to_raw_pointer(m_long_str);
    return ptr;
  }

  /// \brief Erase the current string instance and deallocate used memory.
  template <typename allocator_type>
  void clear(const allocator_type &alloc) {
    priv_deallocate_str(alloc);
  }

  /// \brief Replace the current string with a new one.
  template <typename allocator_type>
  void assign(const char_type *s, const size_type count,
              const allocator_type &alloc) {
    clear(alloc);
    priv_allocate_str(s, count, alloc);
  }

  /// \brief Returns the length of the current string.
  std::size_t size() const { return m_str_length; }

  /// \brief Returns the length of the current string.
  std::size_t length() const { return m_str_length; }

  /// \brief Return `true` if two str-value pairs are equal.
  /// \param lhs A str-value pair to compare.
  /// \param rhs A str-value pair to compare.
  /// \return True if two str-value pairs are equal. Otherwise, false.
  friend bool operator==(const compact_string &lhs,
                         const compact_string &rhs) noexcept {
    return (std::strcmp(lhs.c_str(), rhs.c_str()) == 0);
  }

  /// \brief Return `true` if two str-value pairs are not equal.
  /// \param lhs A str-value pair to compare.
  /// \param rhs A str-value pair to compare.
  /// \return True if two str-value pairs are not equal. Otherwise, false.
  friend bool operator!=(const compact_string &lhs,
                         const compact_string &rhs) noexcept {
    return !(lhs == rhs);
  }

  iterator begin() { return const_cast<char_type *>(c_str()); }

  const_iterator begin() const { return c_str(); }

  iterator end() { return begin() + length(); }

  const_iterator end() const { return begin() + length(); }

 private:
  template <typename other_allocator_type>
  using char_allocator_type = typename std::allocator_traits<
      other_allocator_type>::template rebind_alloc<char_type>;

  static constexpr uint32_t k_short_str_max_length =
      sizeof(char_pointer) - 1;  // -1 for '0'

  bool priv_short_key() const noexcept {
    return (m_str_length <= k_short_str_max_length);
  }

  void priv_clear_variables() {
    if (priv_short_key()) {
      m_short_str_buf = 0;
    } else {
      m_long_str = nullptr;
    }
    m_str_length = 0;
  }

  template <typename alloc_t>
  bool priv_allocate_str(const char_type *const str, const size_type length,
                         const alloc_t &alloc) {
    assert(m_str_length == 0);
    m_str_length = length;

    if (priv_short_key()) {
      std::char_traits<char_type>::copy(m_short_str, str, m_str_length);
      std::char_traits<char_type>::assign(m_short_str[m_str_length], '\0');
    } else {
      char_allocator_type<alloc_t> char_alloc(alloc);
      m_long_str =
          std::allocator_traits<char_allocator_type<alloc_t>>::allocate(
              char_alloc, m_str_length + 1);
      if (!m_long_str) {
        m_str_length = 0;
        std::abort();  // TODO: change
        return false;
      }

      std::char_traits<char_type>::copy(metall::to_raw_pointer(m_long_str), str,
                                        m_str_length);
      std::char_traits<char_type>::assign(m_long_str[m_str_length], '\0');
    }

    return true;
  }

  template <typename alloc_t>
  bool priv_deallocate_str(const alloc_t &alloc) {
    if (!priv_short_key()) {
      char_allocator_type<alloc_t> char_alloc(alloc);
      std::allocator_traits<char_allocator_type<alloc_t>>::deallocate(
          char_alloc, metall::to_raw_pointer(m_long_str), m_str_length + 1);
    }

    priv_clear_variables();

    return true;
  }

  union {
    char_type m_short_str[k_short_str_max_length + 1];  // + 1 for '\0'
    static_assert(sizeof(char_type) * (k_short_str_max_length + 1) <=
                      sizeof(uint64_t),
                  "sizeof(m_short_str) is bigger than sizeof(uint64_t)");
    uint64_t m_short_str_buf;

    char_pointer m_long_str{nullptr};
  };
  size_type m_str_length{0};
};

}  // namespace json_bento::jbdtl
