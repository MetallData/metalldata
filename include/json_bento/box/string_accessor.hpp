// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <iostream>
#include <string>
#include <string_view>

#include <json_bento/box/accessor_fwd.hpp>
#include <json_bento/details/compact_string_storage.hpp>

namespace json_bento::jbdtl {

template <typename storage_allocator_type>
class string_accessor {
 private:
  using storage_t = compact_string_storage<storage_allocator_type>;
  using storage_pointer_t =
      typename std::pointer_traits<typename std::allocator_traits<
          storage_allocator_type>::pointer>::template rebind<storage_t>;

 public:
  using char_type      = typename storage_t::char_type;
  using iterator       = typename storage_t::string_type::iterator;
  using const_iterator = typename storage_t::string_type::const_iterator;

  string_accessor(const std::size_t id, storage_t* const storage)
      : m_id(id), m_storage(storage) {}

  string_accessor(const string_accessor&)                = default;
  string_accessor(string_accessor&&) noexcept            = default;
  string_accessor& operator=(const string_accessor&)     = default;
  string_accessor& operator=(string_accessor&&) noexcept = default;

  string_accessor& operator=(const char_type* const s) {
    m_storage->assign(m_id, s, std::strlen(s));
    return *this;
  }

  /// \brief Provides an explicit conversion to std::basic_string.
  /// \return A std::basic_string with the same data.
  explicit operator std::basic_string<char_type>() const {
    return std::basic_string<char_type>(c_str(), size());
  }

  /// \brief Provides an explicit conversion to std::basic_string_view.
  /// \return A std::basic_string_view with the same data.
  explicit operator std::basic_string_view<char_type>() const {
    return std::basic_string_view<char_type>(c_str(), size());
  }

  /// \brief Checks whether the string is empty.
  /// \return True if the string is empty, false otherwise.
  bool empty() const noexcept { return size() == 0; }

  /// \brief Returns the number of CharT elements in the string.
  /// \return The number of CharT elements in the string.
  std::size_t size() const noexcept { return m_storage->at(m_id).size(); }

  /// \brief Returns the number of CharT elements in the string.
  /// \return The number of CharT elements in the string.
  std::size_t length() const noexcept { return m_storage->at(m_id).length(); }

  /// \brief Returns a pointer to a null-terminated character array with data
  /// equivalent to those stored in the string.
  /// Does not return a null-terminated character array if the stored string is
  /// empty.
  /// \return Pointer to the underlying character storage.
  const char_type* c_str() const noexcept {
    return m_storage->at(m_id).c_str();
  }

  /// \brief Returns a pointer to a null-terminated character array with data
  /// equivalent to those stored in the string.
  /// Does not return a null-terminated character array if the stored string is
  /// empty.
  /// \return Pointer to the underlying character storage.
  const char_type* data() const noexcept { return c_str(); }

  /// \brief Removes all characters from the string.
  void clear() { return m_storage->assign(m_id, "", 0); }

  /// \brief Returns an iterator to the beginning.
  /// \return An iterator to the beginning.
  iterator begin() { return m_storage->begin_at(m_id); }

  /// \brief Returns an iterator to the beginning.
  /// \return An iterator to the beginning.
  const_iterator begin() const { return m_storage->begin_at(m_id); }

  /// \brief Returns an iterator to the end.
  /// \return An iterator to the end.
  iterator end() { return m_storage->end_at(m_id); }

  /// \brief Returns an iterator to the end.
  /// \return An iterator to the end.
  const_iterator end() const { return m_storage->end_at(m_id); }

 private:
  std::size_t       m_id{0};
  storage_pointer_t m_storage{nullptr};
};

}  // namespace json_bento::jbdtl

template <typename storage_allocator_type>
std::ostream& operator<<(
    std::ostream&                                                     os,
    const json_bento::jbdtl::string_accessor<storage_allocator_type>& sa) {
  os << sa.c_str();
  return os;
}
