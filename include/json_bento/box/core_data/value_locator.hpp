// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdlib>
#include <limits>

namespace json_bento::jbdtl {

class value_locator {
 public:
  using index_type = uint64_t;

 private:
  enum data_tag : uint8_t {
    null,
    bool_value,
    int64_value,
    uint64_value,
    double_value,
    string_index,
    array_index,
    object_index
  };

  union data_type {
    index_type index;
    bool       bool_value;
    int64_t    int64_value;
    uint64_t   uint64_value;
    double     double_value;
    void       reset() { uint64_value = 0; }
  };

 public:
  static constexpr std::size_t max_index() {
    return std::numeric_limits<index_type>::max();
  }

  value_locator() { reset(); }

  ~value_locator() noexcept = default;

  bool operator==(const value_locator &other) {
    return m_tag == other.m_tag &&
           (is_null() ||
            (is_bool() && m_data.bool_value == other.m_data.bool_value) ||
            (is_int64() && m_data.int64_value == other.m_data.int64_value) ||
            (is_uint64() && m_data.uint64_value == other.m_data.uint64_value) ||
            (is_double() && m_data.double_value == other.m_data.double_value) ||
            (m_data.index == other.m_data.index));
  }

  bool operator!=(const value_locator &other) { return !(*this == other); }

  bool is_null() const { return m_tag == data_tag::null; }

  bool is_bool() const { return m_tag == data_tag::bool_value; }

  bool is_int64() const { return m_tag == data_tag::int64_value; }

  bool is_uint64() const { return m_tag == data_tag::uint64_value; }

  bool is_double() const { return m_tag == data_tag::double_value; }

  bool is_string_index() const { return m_tag == data_tag::string_index; }

  bool is_array_index() const { return m_tag == data_tag::array_index; }

  bool is_object_index() const { return m_tag == data_tag::object_index; }

  bool is_primitive() const {
    return is_bool() || is_int64() || is_uint64() || is_double();
  }

  bool is_index() const {
    return is_string_index() || is_array_index() || is_object_index();
  }

  bool &as_bool() {
    assert(m_tag == data_tag::bool_value);
    return m_data.bool_value;
  }

  int64_t &as_int64() {
    assert(m_tag == data_tag::int64_value);
    return m_data.int64_value;
  }

  uint64_t &as_uint64() {
    assert(m_tag == data_tag::uint64_value);
    return m_data.uint64_value;
  }

  double &as_double() {
    assert(m_tag == data_tag::double_value);
    return m_data.double_value;
  }

  index_type &as_index() {
    assert(is_index());
    return m_data.index;
  }

  bool as_bool() const {
    assert(m_tag == data_tag::bool_value);
    return m_data.bool_value;
  }

  int64_t as_int64() const {
    assert(m_tag == data_tag::int64_value);
    return m_data.int64_value;
  }

  uint64_t as_uint64() const {
    assert(m_tag == data_tag::uint64_value);
    return m_data.uint64_value;
  }

  double as_double() const {
    assert(m_tag == data_tag::double_value);
    return m_data.double_value;
  }

  index_type as_index() const {
    assert(is_index());
    return m_data.index;
  }

  void emplace_null() { m_tag = data_tag::null; }

  bool &emplace_bool() {
    m_tag = data_tag::bool_value;
    return m_data.bool_value;
  }

  int64_t &emplace_int64() {
    m_tag = data_tag::int64_value;
    return m_data.int64_value;
  }

  uint64_t &emplace_uint64() {
    m_tag = data_tag::uint64_value;
    return m_data.uint64_value;
  }

  double &emplace_double() {
    m_tag = data_tag::double_value;
    return m_data.double_value;
  }

  index_type &emplace_string_index() {
    m_tag = data_tag::string_index;
    return m_data.index;
  }

  index_type &emplace_array_index() {
    m_tag = data_tag::array_index;
    return m_data.index;
  }

  index_type &emplace_object_index() {
    m_tag = data_tag::object_index;
    return m_data.index;
  }

  void reset() {
    m_data.reset();
    m_tag = data_tag::null;
  }

 private:
  data_type m_data;
  data_tag  m_tag;
};

}  // namespace json_bento::jbdtl
