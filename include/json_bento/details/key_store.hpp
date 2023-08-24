// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <scoped_allocator>
#include <string_view>
#include <utility>

#include <metall/container/string.hpp>
#include <metall/container/unordered_map.hpp>
#include <metall/utility/hash.hpp>

#include <json_bento/box/core_data/key_locator.hpp>
#include <json_bento/details/compact_string.hpp>

namespace json_bento::jbdtl {

template <typename Alloc = std::allocator<std::byte>>
class key_store {
 public:
  using allocator_type = Alloc;
  using key_type       = std::string_view;

 private:
  template <typename T>
  using other_allocator =
      typename std::allocator_traits<Alloc>::template rebind_alloc<T>;

  template <typename T>
  using other_scoped_allocator =
      std::scoped_allocator_adaptor<other_allocator<T>>;

  using id_type = uint64_t;

  using string_type =
      compact_string<typename std::allocator_traits<allocator_type>::pointer>;
  using map_allocator_type =
      other_scoped_allocator<std::pair<const id_type, string_type>>;
  using map_type =
      metall::container::unordered_map<id_type, string_type, std::hash<id_type>,
                                       std::equal_to<id_type>,
                                       map_allocator_type>;

  /// Used for representing 'invalid key'.
  static constexpr id_type k_max_internal_id =
      std::numeric_limits<id_type>::max();

 public:
  key_store(){};

  explicit key_store(allocator_type allocator) : m_map(allocator) {}

  explicit key_store(const uint64_t        hash_seed,
                     const allocator_type &allocator = allocator_type())
      : m_hash_seed(hash_seed), m_map(allocator) {}

  // Delete all for now.
  // When implement them, make sure to copy compact_string explicitly.
  key_store(const key_store &)            = delete;
  key_store(key_store &&)                 = delete;
  key_store &operator=(const key_store &) = delete;
  key_store &operator=(key_store &&)      = delete;

  ~key_store() { clear(); }

  key_locator find_or_add(const key_type &key) {
    const auto id = priv_find_or_generate_internal_id(key);
    assert(id != k_max_internal_id);
    m_map.emplace(
        std::piecewise_construct, std::forward_as_tuple(id),
        std::forward_as_tuple(key.data(), key.length(), get_allocator()));
    return id;
  }

  key_locator find(const key_type &key) const {
    const auto locator_type = priv_find_internal_id(key);
    return locator_type;
  }

  key_type find(const key_locator &locator_type) const {
    static_assert(std::is_same_v<key_type, typename string_type::view_type>,
                  "Cannot convert");
    assert(m_map.count(locator_type) == 1);
    return m_map.at(locator_type).str_view();
  }

  void clear() {
    for (auto &item : m_map) {
      item.second.clear(m_map.get_allocator());
    }
  }

  std::size_t size() const { return m_map.size(); }

  allocator_type get_allocator() const { return m_map.get_allocator(); }

 private:
  /// \brief Finds the internal ID that corresponds to 'key'.
  /// If this container does not have an element with 'key',
  /// Generate a new internal ID.
  id_type priv_find_or_generate_internal_id(const key_type &key) {
    auto internal_id = priv_find_internal_id(key);
    if (internal_id == k_max_internal_id) {  // Couldn't find
      // Generate a new one.
      internal_id = priv_generate_internal_id(key);
    }
    return internal_id;
  }

  /// \brief Generates a new internal ID for 'key'.
  id_type priv_generate_internal_id(const key_type &key) {
    auto internal_id = priv_hash_key(key, m_hash_seed);

    std::size_t distance = 0;
    while (m_map.count(internal_id) > 0) {
      internal_id = priv_increment_internal_id(internal_id);
      ++distance;
    }
    m_max_id_probe_distance = std::max(distance, m_max_id_probe_distance);

    return internal_id;
  }

  /// \brief Finds the internal ID that corresponds with 'key'.
  /// If this container does not have an element with 'key',
  /// returns k_max_internal_id.
  id_type priv_find_internal_id(const key_type &key) const {
    auto internal_id = priv_hash_key(key, m_hash_seed);

    for (std::size_t d = 0; d <= m_max_id_probe_distance; ++d) {
      const auto itr = m_map.find(internal_id);
      if (itr == m_map.end()) {
        break;
      }

      if (itr->second.str_view() == key) {
        return internal_id;
      }
      internal_id = priv_increment_internal_id(internal_id);
    }

    return k_max_internal_id;  // Couldn't find
  }

  static id_type priv_hash_key(const key_type                 &key,
                               [[maybe_unused]] const uint64_t seed) {
    auto hash = (id_type)metall::mtlldetail::MurmurHash64A(
        key.data(), (int)key.length(), seed);
    if (hash == k_max_internal_id) {
      hash = priv_increment_internal_id(hash);
    }
    assert(hash != k_max_internal_id);
    return hash;
  }

  static id_type priv_increment_internal_id(const id_type id) {
    const auto new_id = (id + 1) % k_max_internal_id;
    assert(new_id != k_max_internal_id);
    return new_id;
  }

  uint64_t    m_hash_seed{123};
  std::size_t m_max_id_probe_distance{0};
  map_type    m_map;
};

}  // namespace json_bento::jbdtl
