#pragma once

#include <memory>
// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <json_bento/box/accessor_fwd.hpp>
#include <json_bento/box/core_data/core_data.hpp>

namespace json_bento::jbdtl {

template <typename core_data_allocator_type>
class key_value_pair_accessor {
 private:
  using self_t      = key_value_pair_accessor<core_data_allocator_type>;
  using core_data_t = core_data<core_data_allocator_type>;
  using core_data_pointer_t =
      typename std::pointer_traits<typename std::allocator_traits<
          core_data_allocator_type>::pointer>::template rebind<core_data_t>;

 public:
  // TODO: hide from user
  using position_type = uint64_t;

  using key_type            = typename core_data_t::key_type;
  using value_accessor_type = value_accessor<core_data_allocator_type>;

  key_value_pair_accessor(position_type       object_position,
                          position_type       element_position,
                          core_data_pointer_t core_data)
      : m_object_position(object_position),
        m_element_position(element_position),
        m_core_data(core_data) {}

  bool operator==(const key_value_pair_accessor &other) {
    return m_core_data == other.m_core_data &&
           m_object_position == other.m_object_position &&
           m_element_position == other.m_element_position;
  }

  bool operator!=(const key_value_pair_accessor &other) {
    return !(*this == other);
  }

  /// \brief Dereference operator.
  /// The purpose of this function is to enable structure dereference operator
  /// (->) in iterators whose value type is key_value_pair_accessor (e.g.,
  /// object_accessor::iterator). Specifically, this operator enables the
  /// following syntax: \code auto it = object_accessor.begin(); it->value() =
  /// ....; \endcode
  key_value_pair_accessor *operator->() { return this; }

  key_type key() const {
    auto kv_locs =
        m_core_data->object_storage.at(m_object_position, m_element_position);
    return m_core_data->key_storage.find(kv_locs.key());
  }

  value_accessor_type value() {
    return value_accessor_type(value_accessor_type::value_type_tag::object,
                               m_object_position, m_element_position,
                               m_core_data);
  }

  const value_accessor_type value() const {
    return value_accessor_type(value_accessor_type::value_type_tag::object,
                               m_object_position, m_element_position,
                               const_cast<self_t *>(this)->m_core_data);
  }

  /// \brief Returns an allocator instance.
  /// \return Allocator instance.
  auto get_allocator() const {
    return m_core_data->object_storage.get_allocator();
  }

 private:
  position_type       m_object_position;
  position_type       m_element_position;  // index inside an object.
  core_data_pointer_t m_core_data;
};

}  // namespace json_bento::jbdtl
