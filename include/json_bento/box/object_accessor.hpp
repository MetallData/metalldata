// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include <json_bento/box/accessor_fwd.hpp>
#include <json_bento/box/core_data/core_data.hpp>

namespace json_bento::jbdtl {

template <typename core_data_allocator_type>
class object_accessor {
 private:
  using core_data_t = core_data<core_data_allocator_type>;
  using self_type   = object_accessor<core_data_allocator_type>;
  template <bool is_const>
  class basic_iterator;
  using box_pointer_t =
      typename std::pointer_traits<typename std::allocator_traits<
          core_data_allocator_type>::pointer>::template rebind<core_data_t>;

 public:
  using key_type            = typename core_data_t::key_type;
  using value_accessor_type = value_accessor<core_data_allocator_type>;
  using key_value_pair_accessor_type =
      key_value_pair_accessor<core_data_allocator_type>;

  using iterator       = basic_iterator<false>;
  using const_iterator = basic_iterator<true>;

  object_accessor(const std::size_t index, box_pointer_t box)
      : m_object_index(index), m_core_data(box) {}

  value_accessor_type operator[](const key_type &key) {
    const auto idx = priv_find(key);
    if (idx != size()) {
      return value_accessor_type(value_accessor_type::value_type_tag::object,
                                 m_object_index, idx, m_core_data);
    }

    // Allocate a new item
    const auto key_loc = m_core_data->key_storage.find_or_add(key);
    m_core_data->object_storage.push_back(
        m_object_index, key_value_pair(key_loc, value_locator()));
    return value_accessor_type(value_accessor_type::value_type_tag::object,
                               m_object_index, size() - 1, m_core_data);
  }

  /// \brief Access a mapped value.
  /// \param key The key of the mapped value to access.
  /// \return A reference to the mapped value associated with 'key'.
  value_accessor_type at(const key_type &key) const {
    return value_accessor_type(value_accessor_type::value_type_tag::object,
                               m_object_index, priv_find(key), m_core_data);
  }

  /// \brief Return true if the key is found.
  /// \return True if found; otherwise, false.
  bool contains(const key_type &key) const { return priv_find(key) != size(); }

  /// \brief Returns to the value associated with the key if it exists.
  /// \return The value associated with the key in std::optional if it exists;
  /// otherwise, empty std::optional (i.e., std::nullopt).
  std::optional<value_accessor_type> if_contains(const key_type &key) const {
    if (contains(key)) return at(key);
    return std::nullopt;
  }

  /// \brief Count the number of elements with a specific key.
  /// \return The number elements with a specific key.
  std::size_t count(const key_type &key) const { return priv_count(key); }

  iterator find(const key_type &key) {
    return iterator(m_object_index, priv_find(key), m_core_data);
  }

  const_iterator find(const key_type &key) const {
    return const_iterator(m_object_index, priv_find(key), m_core_data);
  }

  iterator begin() { return iterator(m_object_index, 0, m_core_data); }

  const_iterator begin() const {
    return const_iterator(m_object_index, 0, m_core_data);
  }

  iterator end() { return iterator(m_object_index, size(), m_core_data); }

  const_iterator end() const {
    return const_iterator(m_object_index, size(), m_core_data);
  }

  std::size_t size() const {
    return m_core_data->object_storage.size(m_object_index);
  }

  /// \brief Returns an allocator instance.
  /// \return Allocator instance.
  auto get_allocator() const {
    return m_core_data->object_storage.get_allocator();
  }

 private:
  /// \brief Find the index of the item associated with key.
  std::size_t priv_find(const key_type &key) const {
    const auto  key_loc = m_core_data->key_storage.find(key);
    std::size_t i       = 0;
    for (; i < m_core_data->object_storage.size(m_object_index); ++i) {
      auto &kv = m_core_data->object_storage.at(m_object_index, i);
      if (kv.key() == key_loc) {
        return i;
      }
    }
    return i;
  }

  std::size_t priv_count(const key_type &key) const {
    const auto  key_loc = m_core_data->key_storage.find(key);
    std::size_t count   = 0;
    for (auto itr = m_core_data->object_storage.begin(m_object_index);
         itr != m_core_data->object_storage.end(m_object_index); ++itr) {
      if (itr->key() == key_loc) {
        ++count;
      }
    }
    return count;
  }

  auto priv_object_storage_end() {
    return m_core_data->object_storage.end(m_object_index);
  }

  auto priv_object_storage_end() const {
    return m_core_data->object_storage.end(m_object_index);
  }

  std::size_t   m_object_index;
  box_pointer_t m_core_data;
};

/// \brief Iterator-like class
template <typename core_data_allocator_type>
template <bool is_const>
class object_accessor<core_data_allocator_type>::basic_iterator {
 public:
  basic_iterator(std::size_t object_index, std::size_t item_index,
                 box_pointer_t box)
      : m_object_index(object_index),
        m_item_index(item_index),
        m_core_data(box) {}

  bool operator==(const basic_iterator &other) const {
    return m_core_data == other.m_core_data &&
           m_object_index == other.m_object_index &&
           m_item_index == other.m_item_index;
  }

  bool operator!=(const basic_iterator &other) const {
    return !(*this == other);
  }

  basic_iterator &operator++() {
    ++m_item_index;
    return (*this);
  }

  basic_iterator operator++(int) {
    basic_iterator<is_const> tmp(*this);
    operator++();
    return tmp;
  }

  /// \brief Dereference operator.
  /// \return Key-value pair accessor.
  /// \warning This function returns a key_value_pair_accessor_type instance
  /// rather than a reference.
  key_value_pair_accessor_type operator*() const {
    return key_value_pair_accessor_type(m_object_index, m_item_index,
                                        m_core_data);
  }

  /// \brief Structure dereference operator.
  /// \return Key-value pair accessor.
  /// \warning This function returns a key_value_pair_accessor_type instance
  /// rather than a pointer. This function expects the
  /// key_value_pair_accessor_type has operator->() defined.
  key_value_pair_accessor_type operator->() const { return operator*(); }

 private:
  std::size_t   m_object_index;
  std::size_t   m_item_index;
  box_pointer_t m_core_data;
};
}  // namespace json_bento::jbdtl
