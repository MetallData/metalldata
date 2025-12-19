// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <cassert>
#include <concepts>
#include <memory>
#include <string>
#include <string_view>

#include <boost/interprocess/offset_ptr.hpp>

namespace compact_string {

/// \brief String view class that uses offset pointer
template <typename char_type, typename pointer> class basic_string_view {
private:
public:
  using size_type = std::size_t;
  using char_pointer =
      typename std::pointer_traits<pointer>::template rebind<const char_type>;

  basic_string_view() = default;

  basic_string_view(const char_type *s)
      : m_data(s), m_length(std::char_traits<char_type>::length(s)) {}

  basic_string_view(const char_type *s, const size_type length)
      : m_data(s), m_length(length) {}

  explicit basic_string_view(std::string_view view)
      : m_data(view.data()), m_length(view.size()) {}

  basic_string_view(const basic_string_view &) = default;

  basic_string_view(basic_string_view &&) noexcept = default;

  basic_string_view &operator=(const basic_string_view &) = default;

  basic_string_view &operator=(basic_string_view &&) noexcept = default;

  ~basic_string_view() = default;

  // equal operator
  bool operator==(const basic_string_view &other) const {
    if (m_length != other.m_length) {
      return false;
    }
    return std::char_traits<char_type>::compare(data(), other.data(),
                                                m_length) == 0;
  }

  // not equal operator
  bool operator!=(const basic_string_view &other) const {
    return !(*this == other);
  }

  const char_type *data() const noexcept { return m_data.get(); }

  std::string_view str_view() const noexcept {
    return std::string_view(data(), m_length);
  }

  size_type size() const noexcept { return m_length; }

  size_type length() const noexcept { return size(); }

private:
  char_pointer m_data{nullptr};
  size_t m_length{0};
};

using string_view = basic_string_view<char, boost::interprocess::offset_ptr<char>>;

} // namespace compact_string