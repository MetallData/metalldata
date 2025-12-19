// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <scoped_allocator>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/container/string.hpp>
#include <boost/functional/hash.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "string_table/string_accessor.hpp"

namespace compact_string {
namespace csdtl {
/// \brief Allocate a string and embed the length of the string in front of the
/// allocaated string.
/// \tparam size_type The type of the length of the string.
/// \tparam allocator_type The type of the allocator.
/// \param str The string to be allocated.
/// \param alloc The allocator.
/// \return A pointer to an allocated buffer. The buffer contains the length of
/// the string followed by the string data.
template<typename size_type, typename allocator_type>
char *allocate_string_embedding_length(const std::string_view &str,
                                       const allocator_type &alloc) {
  using char_allocator = typename std::allocator_traits<
    allocator_type>::template rebind_alloc<char>;
  static_assert(
    std::is_same_v<typename std::allocator_traits<char_allocator>::value_type,
                   char>,
    "char_allocator::value_type must be the same as char");
  char_allocator char_alloc(alloc);

  char *buf =
      std::to_address(char_alloc.allocate(sizeof(size_type) + str.size() + 1));

  auto *size_buf = reinterpret_cast<size_type *>(buf);
  size_buf[0] = str.size();

  auto *str_buf = &buf[sizeof(size_type)];
  std::char_traits<char>::copy(str_buf,
                               std::to_address(str.data()),
                               str.size());
  std::char_traits<char>::assign(str_buf[str.size()], '\0');
  return buf;
}
} // namespace csdtl

template<typename Alloc = std::allocator<std::byte> >
class string_store {
  private:
    using self_type = string_store<Alloc>;

    template<typename T>
    using other_allocator =
    typename std::allocator_traits<Alloc>::template rebind_alloc<T>;

    template<typename T>
    using other_scoped_allocator =
    std::scoped_allocator_adaptor<other_allocator<T> >;

  public:
    using allocator_type = other_allocator<char>;
    using size_type = std::size_t;

  private:
    // Internal pointer types to store that could be offset pointers
    using voild_pointer =
    typename std::allocator_traits<allocator_type>::void_pointer;
    using internal_const_char_pointer =
    std::pointer_traits<voild_pointer>::template rebind<const char>;

    class str_holder {
      public:
        str_holder() = default;

        /// \brief Construct a string holder from a pointer to the string data.
        /// \param str A pointer to the string data. This pointer must point to the
        /// length data followed by actual string data.
        str_holder(const char *const str) : m_ptr(str) {
        }

        str_holder(const str_holder &) = delete;
        str_holder(str_holder &&) noexcept = default;
        str_holder &operator=(const str_holder &) = delete;
        str_holder &operator=(str_holder &&) noexcept = default;

        ~str_holder() = default;

        const char *str() const {
          static_assert(sizeof(char) == 1, "char must be one byte");
          return std::to_address(&m_ptr[sizeof(size_type)]);
        }

        size_type length() const {
          // First entry is the length
          return reinterpret_cast<const size_type *>(std::to_address(m_ptr))[0];
        }

        // equal operator
        bool operator==(const str_holder &other) const {
          if (length() != other.length()) {
            return false;
          }
          return std::char_traits<char>::compare(std::to_address(str()),
                                                 std::to_address(other.str()),
                                                 length()) == 0;
        }

        // not equal operator
        bool operator!=(const str_holder &other) const {
          return !(*this == other);
        }

      private:
        internal_const_char_pointer m_ptr;
    };
    struct str_holder_equal {
      bool operator()(const str_holder &left,
                      const std::string_view &right) const {
        if (left.length() != right.length()) {
          return false;
        }
        return std::char_traits<char>::compare(std::to_address(left.str()),
                                               right.data(),
                                               right.length()) == 0;
      }
      bool operator()(const std::string_view &right,
                      const str_holder &left) const {
        return operator()(left, right);
      }
    };

    // Hash function for basic_string_view
    struct set_hasher {
      using is_transparent = void;

      std::size_t operator()(const str_holder &str) const {
        return boost::hash_range(str.str(), str.str() + str.length());
      }
      std::size_t operator()(const std::string_view &str) const {
        return boost::hash_range(str.data(), str.data() + str.length());
      }
    };

    struct set_equal {
      using is_transparent = void;
      bool operator()(const str_holder &left, const str_holder &right) const {
        return left == right;
      }
      bool operator()(const str_holder &left,
                      const std::string_view &right) const {
        return str_holder_equal()(left, right);
      }

      bool operator()(const std::string_view &left,
                      const str_holder &right) const {
        return str_holder_equal()(right, left);
      }

      bool operator()(const std::string_view &left,
                      const std::string_view &right) const {
        return left == right;
      }
    };

    using set_allocator_type = other_scoped_allocator<str_holder>;
    using set_type = boost::unordered_flat_set<
      str_holder, set_hasher, set_equal,
      set_allocator_type>;

  public:
    /// \brief Get the length of the string
    /// \param str A pointer to actual string data (not the address that points to
    /// the length data).
    static constexpr size_t str_length(const char *const str) {
      const auto *ptr = reinterpret_cast<const size_type *>(str);
      return *(ptr - 1);
    }

    string_store() = default;

    explicit string_store(allocator_type allocator) : m_set(allocator) {
    }

    string_store(const string_store &) = delete;
    string_store(string_store &&) noexcept = default;
    string_store &operator=(const string_store &) = delete;
    string_store &operator=(string_store &&) noexcept = default;

    ~string_store() noexcept = default;

    /// Return the pointer to the string data if the string is found in the store.
    const char *find_or_add(std::string_view str) {
      auto itr = m_set.find(str);
      if (itr != m_set.end()) {
        // Found in the store
        return std::to_address(itr->str());
      }
      // Not found, add it
      char *len_str_buf = priv_allocate_string(str);
      auto ret = m_set.emplace(len_str_buf);
      assert(ret.second); // must be inserted
      const auto &str_holder = *(ret.first);
      assert(str_holder.length() == str.length());
      assert(std::string_view(str_holder.str(), str_holder.length()) ==
        str.data());
      return std::to_address(str_holder.str());
    }

    const char *find(std::string_view str) {
      auto itr = m_set.find(str);
      if (itr == m_set.end()) {
        return nullptr;
      }
      return std::to_address(itr->str());
    }

    std::size_t size() const { return m_set.size(); }

    typename set_type::const_iterator begin() const { return m_set.begin(); }
    typename set_type::const_iterator end() const { return m_set.end(); }

    void clear() {
      for (auto &item : m_set) {
        priv_deallocate_string(item);
      }
      m_set.clear();
    }

    allocator_type get_allocator() const { return m_set.get_allocator(); }

  private:
    char *priv_allocate_string(const std::string_view &str) {
      return csdtl::allocate_string_embedding_length<size_type>(
        str,
        m_set.get_allocator());
    }

    void priv_deallocate_string(const std::string_view &str) {
      static_assert(
        std::is_same_v<
          typename std::allocator_traits<allocator_type>::value_type, char>,
        "allocator_type::value_type must be the same as char");

      std::allocator_traits<allocator_type>::deallocate(
        m_set.get_allocator(),
        const_cast<char *>(str.data()),
        sizeof(size_type) + str.size() + 1);
    }

    set_type m_set;
};

/// \brief Helper function to add a string to the string store
template<typename Alloc>
string_accessor add_string(const char *const str,
                           const std::size_t len,
                           string_store<Alloc> &store) {
  // If string is short, store it in the holder
  // otherwise, store it in the store and store the pointer in the holder
  if (len <= string_accessor::short_str_max_length()) {
    return string_accessor(str, len);
  }
  return string_accessor(store.find_or_add(str), len);
}
} // namespace compact_string
