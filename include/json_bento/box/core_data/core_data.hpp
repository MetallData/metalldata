// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <utility>

#include <metall/container/string.hpp>
#include <metall/container/vector.hpp>

#include <json_bento/box/core_data/value_locator.hpp>
#include <json_bento/details/compact_adjacency_list.hpp>
#include <json_bento/details/compact_string_storage.hpp>
#include <json_bento/details/data_storage.hpp>
#include <json_bento/details/key_store.hpp>
#include <json_bento/details/key_value_pair.hpp>

namespace json_bento::jbdtl {

template <typename Alloc>
struct core_data {
 public:
  using allocator_type = Alloc;
  using string_type    = metall::container::basic_string<
      char, std::char_traits<char>,
      typename std::allocator_traits<allocator_type>::template rebind_alloc<
          char>>;
  using value_locator_type  = value_locator;
  using key_storage_type    = key_store<allocator_type>;
  using key_type            = typename key_storage_type::key_type;
  using string_storage_type = compact_string_storage<allocator_type>;
  using array_storage_type =
      compact_adjacency_list<value_locator_type, allocator_type>;
  using object_storage_type =
      compact_adjacency_list<key_value_pair, allocator_type>;

  // Use vector here to provide vector-like concept in JSON Bento
  using root_value_storage_type =
      metall::container::vector<value_locator_type,
                                typename std::allocator_traits<allocator_type>::
                                    template rebind_alloc<value_locator_type>>;

  core_data() = default;

  explicit core_data(allocator_type alloc)
      : string_storage(alloc),
        root_value_storage(alloc),
        array_storage(alloc),
        object_storage(alloc),
        key_storage(alloc) {}

  ~core_data() noexcept = default;

  string_storage_type     string_storage{allocator_type{}};
  root_value_storage_type root_value_storage{allocator_type{}};
  array_storage_type      array_storage{allocator_type{}};
  object_storage_type     object_storage{allocator_type{}};
  key_storage_type        key_storage{allocator_type{}};
};

}  // namespace json_bento::jbdtl

// TODO: make a better implementation
namespace json_bento::jbdtl {

template <typename value_type, typename core_data_type>
inline void add_value(const value_type &value, core_data_type &core_data,
                      value_locator &loc) {
  if (value.is_null()) {
    loc.reset();
  } else if (value.is_bool()) {
    loc.emplace_bool() = value.as_bool();
  } else if (value.is_int64()) {
    loc.emplace_int64() = value.as_int64();
  } else if (value.is_uint64()) {
    loc.emplace_uint64() = value.as_uint64();
  } else if (value.is_double()) {
    loc.emplace_double() = value.as_double();
  } else if (value.is_string()) {
    loc.emplace_string_index() = core_data.string_storage.emplace(
        value.as_string().c_str(), value.as_string().size());
  } else if (value.is_array()) {
    const auto row = core_data.array_storage.push_back();
    for (const auto &v : value.as_array()) {
      core_data.array_storage.push_back(row, value_locator());
      const auto col = core_data.array_storage.size(row) - 1;
      add_value(v, core_data, core_data.array_storage.at(row, col));
    }
    loc.emplace_array_index() = row;
  } else if (value.is_object()) {
    const auto row = core_data.object_storage.push_back();
    for (const auto &kv : value.as_object()) {
#if BOOST_VERSION >= 107900
      const auto key_loc = core_data.key_storage.find_or_add(kv.key());
#else
      const auto key_loc = core_data.key_storage.find_or_add(kv.key().data());
#endif
      core_data.object_storage.push_back(
          row, key_value_pair(key_loc, value_locator()));
      const auto col = core_data.object_storage.size(row) - 1;
      add_value(kv.value(), core_data,
                core_data.object_storage.at(row, col).value());
    }
    loc.emplace_object_index() = row;
  } else {
    assert(false);
  }
}

/// \brief Add a value at the end of a core data as a root value.
/// \tparam value_type JSON value type that is compatible with Boost/Metall JSON
/// value.
/// \tparam core_data_type Core data type.
/// \param source_value A value to be added.
/// \param core_data A core data to which the value is added.
/// \return The index of the added value.
template <typename value_type, typename core_data_type>
inline auto push_back_root_value(const value_type &source_value,
                                 core_data_type   &core_data) {
  const auto idx = core_data.root_value_storage.size();
  core_data.root_value_storage.emplace_back();
  add_value(source_value, core_data, core_data.root_value_storage.back());
  return idx;
}

}  // namespace json_bento::jbdtl
