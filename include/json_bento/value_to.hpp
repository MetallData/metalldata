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
} // namespace

namespace jbdtl {

template <typename allocator_type>
inline void value_to_helper(const value_accessor<allocator_type> &jv,
                            bj::value &out_bj_value) {
  if (jv.is_bool()) {
    out_bj_value = jv.as_bool();
  } else if (jv.is_int64()) {
    out_bj_value = jv.as_int64();
  } else if (jv.is_uint64()) {
    out_bj_value = jv.as_uint64();
  } else if (jv.is_double()) {
    out_bj_value = jv.as_double();
  } else if (jv.is_string()) {
    out_bj_value = jv.as_string().c_str();
  } else if (jv.is_array()) {
    bj::array bj_array;
    const auto &arr = jv.as_array();
    for (std::size_t i = 0; i < arr.size(); ++i) {
      bj_array.template emplace_back(value_to(arr[i]));
    }
    out_bj_value = bj_array;
  } else if (jv.is_object()) {
    bj::object bj_object;
    const auto obj = jv.as_object();
    for (const auto &kv : obj) {
  #if BOOST_VERSION >= 107900
      bj_object[kv.key()] = value_to(kv.value());
#else
      bj_object[kv.key().data()] = value_to(kv.value());
#endif
    }
    out_bj_value = bj_object;
  } else if (jv.is_null()) {
    out_bj_value.emplace_null();
  }
}

template <typename allocator_type>
inline boost::json::value value_to(const value_accessor<allocator_type> &jv) {
  bj::value out_value;
  value_to_helper(jv, out_value);
  return out_value;
}

} // namespace jbdtl
} // namespace json_bento

namespace json_bento {

/// \brief Convert a value_accessor to the type T.
/// Currently, only Boost.JSON value type is supported as T.
/// \tparam T The type to convert to.
/// \tparam allocator_type The allocator type used in the value_accessor.
/// \param value The value_accessor to convert.
/// \return The converted value.
template <typename T, typename allocator_type>
inline T value_to(const jbdtl::value_accessor<allocator_type> &value) {
  return jbdtl::value_to(value);
}
} // namespace json_bento
