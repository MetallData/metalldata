// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cassert>

#include <json_bento/boost_json.hpp>
#include <json_bento/box/accessor_fwd.hpp>

namespace json_bento::jbdtl {

namespace {
namespace bj = boost::json;
}  // namespace

template <typename T, typename allocator_type>
inline void value_from(const T                              &value,
                       jbdtl::value_accessor<allocator_type> accessor) {
  if (value.is_bool()) {
    accessor = value.as_bool();
  } else if (value.is_int64()) {
    accessor = value.as_int64();
  } else if (value.is_uint64()) {
    accessor = value.as_uint64();
  } else if (value.is_double()) {
    accessor = value.as_double();
  } else if (value.is_string()) {
    accessor = value.as_string().c_str();
  } else if (value.is_array()) {
    const auto src_arr = value.as_array();
    auto       trg_arr = accessor.emplace_array();
    trg_arr.resize(value.as_array().size());
    for (std::size_t i = 0; i < src_arr.size(); ++i) {
      auto elem = trg_arr[i];
      value_from(src_arr[i], elem);
    }
  } else if (value.is_object()) {
    const auto src_obj = value.as_object();
    auto       trg_obj = accessor.emplace_object();
    for (const auto &kv : src_obj) {
      auto elem = trg_obj[kv.key().data()];
      value_from(kv.value(), elem);
    }
  } else if (value.is_null()) {
    accessor.emplace_null();
  } else {
    assert(false);
  }
}

}  // namespace json_bento::jbdtl

namespace json_bento {

/// \brief Convert a value to a JSON value accessor.
/// \tparam T The type of the value to convert.
/// Assume that T has Boost.JSON or Metall JSON compatible interface.
/// \tparam allocator_type The allocator type of the JSON value accessor.
/// \param value The value to convert.
/// \param accessor The JSON value accessor to convert to.
template <typename T, typename allocator_type>
inline void value_from(const T                              &value,
                       jbdtl::value_accessor<allocator_type> accessor) {
  return jbdtl::value_from(value, accessor);
}
}  // namespace json_bento