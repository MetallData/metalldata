// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>

#include <json_bento/boost_json.hpp>
#include <json_bento/box/accessor_fwd.hpp>
#include <json_bento/box/core_data/core_data.hpp>
#include <json_bento/value_to.hpp>

namespace json_bento::jbdtl {

/// \brief Array accessor.
/// This class provides similar API as metall::json_bento::array.
/// \tparam core_data_allocator_type Allocator type of the Bento core_data.
template <typename core_data_allocator_type>
class array_accessor {
 private:
  using self_type     = array_accessor<core_data_allocator_type>;
  using core_data_t   = core_data<core_data_allocator_type>;
  using value_locator = typename core_data_t::value_locator_type;
  using core_data_pointer_t =
      typename std::pointer_traits<typename std::allocator_traits<
          core_data_allocator_type>::pointer>::template rebind<core_data_t>;
  template <bool is_const>
  class basic_iterator;

 public:
  using value_accessor_type = value_accessor<core_data_allocator_type>;
  using iterator            = basic_iterator<false>;
  using const_iterator      = basic_iterator<true>;

  array_accessor(const std::size_t index, core_data_pointer_t core_data)
      : m_array_index(index), m_core_data(core_data) {}

  value_accessor_type operator[](const std::size_t position) {
    return value_accessor_type(value_accessor_type::value_type_tag::array,
                               m_array_index, position, m_core_data);
  }

  const value_accessor_type operator[](const std::size_t position) const {
    return (*const_cast<self_type *>(this))[position];
  }

  value_accessor_type back() { return this->operator[](size() - 1); }

  const value_accessor_type back() const {
    return this->operator[](size() - 1);
  }

  std::size_t size() const {
    return m_core_data->array_storage.size(m_array_index);
  }

  /// \brief Resize the array.
  /// \param size New size.
  void resize(const std::size_t size) {
    m_core_data->array_storage.resize(m_array_index, size);
  }

  /// \brief Add an element to the end of the array.
  /// Expand (resize) the array if capacity() < size() + 1.
  /// \param value Value to add.
  void push_back(value_accessor_type value) {
    // TODO: implement more efficient one
    value_locator loc;
    add_value(json_bento::value_to<boost::json::value>(value), *m_core_data,
              loc);
    m_core_data->array_storage.push_back(m_array_index, std::move(loc));
  }

  /// \brief Append a constructed element in-place.
  /// \tparam Arg Argument type.
  /// \param arg Argument to construct the element.
  /// \return Value accessor to the added element.
  template <typename Arg>
  value_accessor_type emplace_back(Arg &&arg) {
    boost::json::value value(std::forward<Arg>(arg));
    value_locator      loc;
    add_value(value, *m_core_data, loc);
    m_core_data->array_storage.push_back(m_array_index, std::move(loc));
    return back();
  }

  iterator begin() { return iterator(m_array_index, 0, m_core_data); }

  const_iterator begin() const {
    return const_iterator(m_array_index, 0, m_core_data);
  }

  iterator end() { return iterator(m_array_index, size(), m_core_data); }

  const_iterator end() const {
    return const_iterator(m_array_index, size(), m_core_data);
  }

  /// \brief Returns an allocator instance.
  /// \return Allocator instance.
  auto get_allocator() const {
    return m_core_data->array_storage.get_allocator();
  }

 private:
  std::size_t         m_array_index;
  core_data_pointer_t m_core_data;
};

/// \brief Iterator-like class
template <typename core_data_allocator_type>
template <bool is_const>
class array_accessor<core_data_allocator_type>::basic_iterator {
 public:
  basic_iterator(std::size_t array_index, std::size_t position,
                 core_data_pointer_t core_data)
      : m_array_index(array_index),
        m_position(position),
        m_core_data(core_data) {}

  bool operator==(const basic_iterator &other) const {
    return m_core_data == other.m_core_data &&
           m_array_index == other.m_array_index &&
           m_position == other.m_position;
  }

  bool operator!=(const basic_iterator &other) const {
    return !(*this == other);
  }

  basic_iterator &operator--() {
    --m_position;
    return (*this);
  }

  basic_iterator &operator++() {
    ++m_position;
    return (*this);
  }

  basic_iterator operator--(int) {
    basic_iterator<is_const> tmp(*this);
    operator--();
    return tmp;
  }

  basic_iterator operator++(int) {
    basic_iterator<is_const> tmp(*this);
    operator++();
    return tmp;
  }

  /// \brief Dereference operator.
  /// \return Value accessor.
  /// \warning This function returns a value_accessor_type instance rather than
  /// a reference.
  value_accessor_type operator*() const {
    return value_accessor_type(value_accessor_type::value_type_tag::array,
                               m_array_index, m_position, m_core_data);
  }

  /// \brief Structure dereference operator.
  /// \return Value accessor.
  /// \warning This function returns a value_accessor_type instance rather than
  /// a pointer. This function expects the value_accessor_type has operator->()
  /// defined.
  value_accessor_type operator->() const { return operator*(); }

 private:
  std::size_t         m_array_index;
  std::size_t         m_position;
  core_data_pointer_t m_core_data;
};
}  // namespace json_bento::jbdtl
