// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <string_view>

#include <boost/json/src.hpp>

#include <json_bento/box/accessor_fwd.hpp>
#include <json_bento/box/core_data/core_data.hpp>
#include <json_bento/value_to.hpp>

namespace json_bento::jbdtl {

// TODO: implement const accessor
template <typename core_data_allocator_type>
class value_accessor {
 private:
  using self_type   = value_accessor<core_data_allocator_type>;
  using core_data_t = core_data<core_data_allocator_type>;
  using box_pointer_t =
      typename std::pointer_traits<typename std::allocator_traits<
          core_data_allocator_type>::pointer>::template rebind<core_data_t>;
  using value_locator_t = typename core_data_t::value_locator_type;
  using value_locator_pointer_t =
      typename std::pointer_traits<typename std::allocator_traits<
          core_data_allocator_type>::pointer>::template rebind<value_locator_t>;

 public:
  using string_type = typename core_data_t::string_type;
  using object_accessor =
      json_bento::jbdtl::object_accessor<core_data_allocator_type>;
  using array_accessor =
      json_bento::jbdtl::array_accessor<core_data_allocator_type>;
  using string_accessor =
      json_bento::jbdtl::string_accessor<core_data_allocator_type>;

  /// \note the following type is not intended to be exposed users and
  /// will be removed from the public interface.
  using position_type = std::size_t;  // TODO: move to private

  /// \note the following type is not intended to be exposed users and
  /// will be removed from the public interface.
  enum value_type_tag {  // TODO: move to private
    invalid,
    root,   // root value
    array,  // value in an array
    object  // value in an object
  };

  value_accessor(value_type_tag tag, position_type pos0, box_pointer_t box)
      : m_tag(tag), m_pos0(pos0), m_box(box) {}

  value_accessor(value_type_tag tag, position_type pos0, position_type pos1,
                 box_pointer_t box)
      : m_tag(tag), m_pos0(pos0), m_pos1(pos1), m_box(box) {}

  /// \brief Dereference operator.
  /// The purpose of this function is to enable structure dereference operator
  /// (->) in iterators whose value type is value_accessor (e.g.,
  /// array_accessor::iterator). Specifically, this operator enables the
  /// following syntax: \code auto it = array_accessor.begin(); it->as_bool() =
  /// ...; \endcode
  value_accessor *operator->() { return this; }

  /// \brief Assign a bool value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const bool b) {
    emplace_bool() = b;
    return *this;
  }

  /// \brief Assign a char value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const signed char i) {
    return operator=(static_cast<long long>(i));
  }

  /// \brief Assign a short value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const short i) {
    return operator=(static_cast<long long>(i));
  }

  /// \brief Assign an int value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const int i) {
    return operator=(static_cast<long long>(i));
  }

  /// \brief Assign a long value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const long i) {
    return operator=(static_cast<long long>(i));
  }

  /// \brief Assign a long long value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const long long i) {
    emplace_int64() = i;
    return *this;
  }

  /// \brief Assign an unsigned char value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const unsigned char u) {
    return operator=(static_cast<unsigned long long>(u));
  }

  /// \brief Assign an unsigned short value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const unsigned short u) {
    return operator=(static_cast<unsigned long long>(u));
  }

  /// \brief Assign an unsigned int value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const unsigned int u) {
    return operator=(static_cast<unsigned long long>(u));
  }

  /// \brief Assign an unsigned long value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const unsigned long u) {
    return operator=(static_cast<unsigned long long>(u));
  }

  /// \brief Assign an unsigned long long value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const unsigned long long u) {
    emplace_uint64() = u;
    return *this;
  }

  /// \brief Assign a null value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(std::nullptr_t) {
    emplace_null();
    return *this;
  }

  /// \brief Assign a double value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const double d) {
    emplace_double() = d;
    return *this;
  }

  /// \brief Assign a std::string_view value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(std::string_view s) {
    emplace_string() = s.data();
    return *this;
  }

  /// \brief Assign a const char* value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const char *const s) {
    emplace_string() = s;
    return *this;
  }

  /// \brief Assign a string_type value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const string_type &s) {
    emplace_string() = s;
    return *this;
  }

  // value_accessor &operator=(std::initializer_list<value_ref> init);

  /// \brief Assign a string_type value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(string_type &&s) {
    emplace_string() = std::move(s);
    return *this;
  }

  /// \brief Assign an array_accessor value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const array_accessor &arr) {
    emplace_array() = arr;
    return *this;
  }

  /// \brief Assign an array_accessor value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(array_accessor &&arr) {
    emplace_array() = std::move(arr);
    return *this;
  }

  /// \brief Assign an object_type value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(const object_accessor &obj) {
    emplace_object() = obj;
    return *this;
  }

  /// \brief Assign an object_type value.
  /// Allocates a memory storage or destroy the old content, if necessary.
  value_accessor &operator=(object_accessor &&obj) {
    emplace_object() = std::move(obj);
    return *this;
  }

  /// \brief Return true if this is a null.
  bool is_null() const { return get_locator().is_null(); }

  /// \brief Return true if this is a bool.
  bool is_bool() const { return get_locator().is_bool(); }

  /// \brief Return true if this is a int64.
  bool is_int64() const { return get_locator().is_int64(); }

  /// \brief Return true if this is a uint64.
  bool is_uint64() const { return get_locator().is_uint64(); }

  /// \brief Return true if this is a double.
  bool is_double() const { return get_locator().is_double(); }

  /// \brief Return true if this is a string.
  bool is_string() const { return get_locator().is_string_index(); }

  /// \brief Return true if this is an array.
  bool is_array() const { return get_locator().is_array_index(); }

  /// \brief Return true if this is a object.
  bool is_object() const { return get_locator().is_object_index(); }

  /// \brief Return true if this is a bool.
  bool &as_bool() {
    return const_cast<bool &>(const_cast<const self_type *>(this)->as_bool());
  }

  /// \brief Return a reference to the held value as a bool.
  const bool &as_bool() const {
    assert(is_bool());
    return get_locator().as_bool();
  }

  /// \brief Return a reference to the held value as a int64.
  int64_t &as_int64() {
    return const_cast<int64_t &>(
        const_cast<const self_type *>(this)->as_int64());
  }

  /// \brief Return a reference to the held value as a int64.
  const int64_t &as_int64() const {
    assert(is_int64());
    return get_locator().as_int64();
  }

  /// \brief Return a reference to the held value as a uint64.
  uint64_t &as_uint64() {
    return const_cast<uint64_t &>(
        const_cast<const self_type *>(this)->as_uint64());
  }

  /// \brief Return a reference to the held value as a uint64.
  const uint64_t &as_uint64() const {
    assert(is_uint64());
    return get_locator().as_uint64();
  }

  /// \brief Return a reference to the held value as a double.
  double &as_double() {
    return const_cast<double &>(
        const_cast<const self_type *>(this)->as_double());
  }

  /// \brief Return a reference to the held value as a double.
  const double &as_double() const {
    assert(is_double());
    return get_locator().as_double();
  }

  /// \brief Return a reference to the held value as a string.
  string_accessor as_string() {
    return const_cast<const self_type *>(this)->as_string();
  }

  /// \brief Return a reference to the held value as a string.
  const string_accessor as_string() const {
    assert(is_string());
    const auto index = get_locator().as_index();
    return string_accessor(index, &m_box->string_storage);
  }

  /// \brief Return a reference to the held value as a array.
  array_accessor as_array() {
    return const_cast<const self_type *>(this)->as_array();
  }

  /// \brief Return a reference to the held value as a array.
  const array_accessor as_array() const {
    assert(is_array());
    const auto index = get_locator().as_index();
    return array_accessor(index, m_box);
  }

  /// \brief Return a reference to the held value as a object.
  object_accessor as_object() {
    return const_cast<const self_type *>(this)->as_object();
  }

  /// \brief Return a reference to the held value as a object.
  const object_accessor as_object() const {
    assert(is_object());
    const auto index = get_locator().as_index();
    return object_accessor(index, const_cast<self_type *>(this)->m_box);
  }

  /// \brief Erase the existing value and reset it to null.
  void emplace_null() { priv_reset(); }

  /// \brief Erase the existing value and reset it to bool.
  /// \return A reference to the new value as bool.
  bool &emplace_bool() {
    priv_reset();
    return get_locator().emplace_bool();
  }

  /// \brief Erase the existing value and reset it to int64.
  /// \return A reference to the new value as int64.
  int64_t &emplace_int64() {
    priv_reset();
    return get_locator().emplace_int64();
  }

  /// \brief Erase the existing value and reset it to uint64.
  /// \return A reference to the new value as uint64.
  uint64_t &emplace_uint64() {
    priv_reset();
    return get_locator().emplace_uint64();
  }

  /// \brief Erase the existing value and reset it to double.
  /// \return A reference to the new value as double.
  double &emplace_double() {
    priv_reset();
    return get_locator().emplace_double();
  }

  /// \brief Erase the existing value and reset it to string.
  /// \return A string accessor instance to the new value as string.
  /// \note The string accessor is invalidated if the value is modified.
  string_accessor emplace_string() {
    priv_reset();
    const auto index                     = m_box->string_storage.emplace();
    get_locator().emplace_string_index() = index;
    return string_accessor(index, &m_box->string_storage);
  }

  /// \brief Erase the existing value and reset it to array.
  /// \return An array accessor instance to the new value as array.
  array_accessor emplace_array() {
    priv_reset();
    const auto index                    = m_box->array_storage.push_back();
    get_locator().emplace_array_index() = index;
    return array_accessor(index, m_box);
  }

  /// \brief Erase the existing value and reset it to object.
  /// \return An object accessor instance to the new value as object.
  object_accessor emplace_object() {
    priv_reset();
    const auto index                     = m_box->object_storage.push_back();
    get_locator().emplace_object_index() = index;
    return object_accessor(index, m_box);
  }

  /// \brief Parses a JSON represented as a string,
  /// and replaces the existing value with the parsed one.
  /// \param input_json_string Input JSON string.
  void parse(std::string_view input_json_string) {
    boost::json::error_code ec;
    auto bj_value = boost::json::parse(input_json_string.data(), ec);
    if (ec) {
      std::cerr << "Failed to parse: " << ec.message() << std::endl;
      emplace_null();
      return;
    }

    add_value(bj_value, *m_box, get_locator());
  }

  /// \brief Returns an allocator instance.
  /// \return Allocator instance.
  auto get_allocator() const {
    return m_box->root_value_storage.get_allocator();
  }

  /// \brief Equal operator.
  friend bool operator==(const value_accessor &lhs,
                         const value_accessor &rhs) noexcept {
    // TODO: improve efficiency
    return json_bento::value_to<boost::json::value>(lhs) ==
           json_bento::value_to<boost::json::value>(rhs);
  }

  /// \brief Return `true` if two values are not equal.
  /// Two values are equal when they are the
  /// same kind and their referenced values
  /// are equal, or when they are both integral
  /// types and their integral representations are equal.
  friend bool operator!=(const value_accessor &lhs,
                         const value_accessor &rhs) noexcept {
    return !(lhs == rhs);
  }

 private:
  value_locator &get_locator() const {
    if (m_tag == value_type_tag::root) {
      return m_box->root_value_storage.at(m_pos0);
    } else if (m_tag == value_type_tag::array) {
      return m_box->array_storage.at(m_pos0, m_pos1);
    } else if (m_tag == value_type_tag::object) {
      return m_box->object_storage.at(m_pos0, m_pos1).value();
    }
    assert(false);
  }

  void priv_reset() {
    if (get_locator().is_null() || get_locator().is_primitive()) {
    } else if (get_locator().is_string_index()) {
      m_box->string_storage.erase(get_locator().as_index());
    } else if (get_locator().is_array_index()) {
      m_box->array_storage.clear(get_locator().as_index());
      m_box->array_storage.shrink_to_fit(get_locator().as_index());
    } else if (get_locator().is_object_index()) {
      m_box->object_storage.clear(get_locator().as_index());
      m_box->object_storage.shrink_to_fit(get_locator().as_index());
    } else {
      assert(false);
    }
    get_locator().reset();
  }

  value_type_tag m_tag{value_type_tag::invalid};
  position_type  m_pos0{0};
  position_type  m_pos1{0};
  box_pointer_t  m_box{nullptr};
};

}  // namespace json_bento::jbdtl
