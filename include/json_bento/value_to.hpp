// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <json_bento/boost_json.hpp>
#include <json_bento/box/accessor_fwd.hpp>

namespace json_bento {

namespace {
namespace bj = boost::json;
}  // namespace

namespace jbdtl {

/// Assumes that T has Boost.JSON or Metall JSON compatible interface.
template <typename allocator_type, typename T>
inline void value_to_helper(const value_accessor<allocator_type> &jv,
                            T                                    &out_value) {
  if (jv.is_bool()) {
    out_value = jv.as_bool();
  } else if (jv.is_int64()) {
    out_value = jv.as_int64();
  } else if (jv.is_uint64()) {
    out_value = jv.as_uint64();
  } else if (jv.is_double()) {
    out_value = jv.as_double();
  } else if (jv.is_string()) {
    out_value = jv.as_string().c_str();
  } else if (jv.is_array()) {
    auto       &out_array = out_value.emplace_array();
    const auto &arr       = jv.as_array();
    out_array.resize(arr.size());
    for (std::size_t i = 0; i < arr.size(); ++i) {
      value_to_helper(arr[i], out_array[i]);
    }
  } else if (jv.is_object()) {
    auto      &trg_obj = out_value.emplace_object();
    const auto obj     = jv.as_object();
    for (const auto &kv : obj) {
#if BOOST_VERSION >= 107900
      value_to_helper(kv.value(), trg_obj[kv.key()]);
#else
      value_to_helper(kv.value(), trg_obj[kv.key().data()]);
#endif
    }
  } else if (jv.is_null()) {
    out_value.emplace_null();
  }
}

}  // namespace jbdtl
}  // namespace json_bento

namespace json_bento {

/// \brief Convert a value_accessor to the type T.
/// Assume that A) T has Boost.JSON or Metall JSON compatible interface and B) T
/// has a default constructor. \tparam T The type to convert to. \tparam
/// allocator_type The allocator type used in the value_accessor. \param value
/// The value_accessor to convert. \return The converted value.
template <typename T, typename allocator_type>
inline T value_to(const jbdtl::value_accessor<allocator_type> &value) {
  T out_value;
  jbdtl::value_to_helper(value, out_value);
  return out_value;
}

/// \brief Convert a value_accessor to the type T.
/// Assume that T has Boost.JSON or Metall JSON compatible interface.
/// \tparam T The type to convert to.
/// \tparam allocator_type The allocator type used in the value_accessor.
/// \param value The value_accessor to convert.
/// \param out_value The instance for holding converted value.
template <typename T, typename allocator_type>
inline void value_to(const jbdtl::value_accessor<allocator_type> &value,
                     T                                           &out_value) {
  jbdtl::value_to_helper(value, out_value);
}

}  // namespace json_bento
