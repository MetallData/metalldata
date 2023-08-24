// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <scoped_allocator>

#include <json_bento/details/compact_string.hpp>
#include <json_bento/details/data_storage.hpp>

namespace json_bento::jbdtl {

template <typename Alloc = std::allocator<std::byte>>
class compact_string_storage {
 private:
  using compact_string_type =
      compact_string<typename std::allocator_traits<Alloc>::pointer>;
  using storage_type = data_storage<compact_string_type, Alloc>;

 public:
  // using view_type = typename compact_string_type::view_type;
  using string_type     = compact_string_type;
  using char_type       = typename string_type::char_type;
  using allocator_type  = Alloc;
  using iterator        = typename storage_type::iterator;
  using const_iterator  = typename storage_type::const_iterator;
  using size_type       = std::size_t;
  using reference       = string_type &;
  using const_reference = const string_type &;

  compact_string_storage() = default;

  explicit compact_string_storage(const allocator_type &alloc)
      : m_storage(alloc) {}

  ~compact_string_storage() noexcept { clear(); }

  compact_string_storage(const compact_string_storage &)     = default;
  compact_string_storage(compact_string_storage &&) noexcept = default;

  compact_string_storage &operator=(const compact_string_storage &) = default;
  compact_string_storage &operator=(compact_string_storage &&) noexcept =
      default;

  const_reference operator[](const std::size_t id) const {
    return this->at(id);
  };

  const_reference at(const std::size_t id) const { return m_storage.at(id); }

  std::size_t emplace() {
    const auto id = m_storage.emplace();
    return id;
  }

  std::size_t emplace(const char_type *s, size_type count) {
    const auto id = m_storage.emplace(s, count, get_allocator());
    return id;
  }

  std::size_t emplace(const std::string_view &s) {
    const auto id = emplace(s.data(), s.size());
    return id;
  }

  void assign(const std::size_t id, const char_type *s, size_type count) {
    m_storage[id].assign(s, count, get_allocator());
  }

  void assign(const std::size_t id, const std::string_view &s) {
    m_storage[id].assign(s.data(), s.size(), get_allocator());
  }

  void erase(const std::size_t id) {
    m_storage[id].clear(m_storage.get_allocator());
    m_storage.erase(id);
  }

  void clear() {
    for (auto &item : m_storage) {
      item.clear(m_storage.get_allocator());
    }
  }

  std::size_t size() const { return m_storage.size(); }

  const_iterator begin() const { return m_storage.begin(); }

  const_iterator end() const { return m_storage.end(); }

  typename string_type::iterator begin_at(const std::size_t id) {
    return m_storage.at(id).begin();
  }

  typename string_type::const_iterator begin_at(const std::size_t id) const {
    return m_storage.at(id).cbegin();
  }

  typename string_type::iterator end_at(const std::size_t id) {
    return m_storage.at(id).end();
  }

  typename string_type::const_iterator end_at(const std::size_t id) const {
    return m_storage.at(id).cend();
  }

  allocator_type get_allocator() const { return m_storage.get_allocator(); }

 private:
  bool priv_check_empty_space() const {
    return (m_storage.capacity() - m_storage.size() > 0);
  }

  storage_type m_storage{};
};

}  // namespace json_bento::jbdtl
