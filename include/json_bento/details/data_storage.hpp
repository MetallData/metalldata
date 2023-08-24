// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cassert>
#include <memory>
#include <scoped_allocator>

#include <metall/container/set.hpp>

#include <json_bento/details/vector.hpp>

namespace json_bento::jbdtl {

/// \brief Container for storing type T value.
/// This container returns ID when a new element is added.
/// When an element is erased, this container reuse the location and ID for
/// later element insertion.
/// \tparam T Type of the value to insert.
/// \tparam Alloc Allocator type.
template <typename T, typename Alloc = std::allocator<T>>
class data_storage {
 private:
  using storage_alloc_type = std::scoped_allocator_adaptor<
      typename std::allocator_traits<Alloc>::template rebind_alloc<T>>;
  using storage_type = vector<T, storage_alloc_type>;
  using free_slot_storage_type =
      metall::container::set<std::size_t, std::less<std::size_t>,
                             typename std::allocator_traits<
                                 Alloc>::template rebind_alloc<std::size_t>>;
  using void_pointer = typename std::allocator_traits<Alloc>::void_pointer;
  using const_void_pointer =
      typename std::allocator_traits<Alloc>::const_void_pointer;

 public:
  using value_type     = T;
  using allocator_type = Alloc;
  using pointer =
      typename std::pointer_traits<void_pointer>::template rebind<T>;
  using const_pointer =
      typename std::pointer_traits<const_void_pointer>::template rebind<T>;
  using reference       = T &;
  using const_reference = const value_type &;

  template <bool is_const>
  class basic_iterator;
  using iterator       = basic_iterator<false>;
  using const_iterator = basic_iterator<true>;

  data_storage() = default;

  explicit data_storage(const allocator_type &alloc)
      : m_storage(alloc), m_free_slots(alloc) {}

  data_storage(const std::size_t size, const allocator_type &alloc)
      : m_storage(size, alloc), m_free_slots(alloc) {}

  ~data_storage() noexcept = default;

  data_storage(const data_storage &)     = default;
  data_storage(data_storage &&) noexcept = default;

  data_storage &operator=(const data_storage &)     = default;
  data_storage &operator=(data_storage &&) noexcept = default;

  reference operator[](const std::size_t id) { return this->at(id); };

  const_reference operator[](const std::size_t id) const {
    return this->at(id);
  };

  reference at(const std::size_t id) { return m_storage.at(id); }

  const_reference at(const std::size_t id) const { return m_storage.at(id); }

  template <typename... Args>
  std::size_t emplace(Args &&...args) {
    if (m_free_slots.empty()) {
      // Allocates a new slot.
      m_storage.emplace_back(std::forward<Args>(args)...);
      return m_storage.size() - 1;
    }

    // Reuse previously cleared slot.
    const auto empty_slot_pos = *(m_free_slots.begin());
    new (&(m_storage.at(empty_slot_pos)))
        value_type(std::forward<Args>(args)...);
    m_free_slots.erase(m_free_slots.begin());
    return empty_slot_pos;
  }

  std::size_t size() const { return m_storage.size() - m_free_slots.size(); }

  std::size_t capacity() const { return m_storage.size(); }

  void erase(const std::size_t id) {
    m_storage.at(id).~value_type();
    m_free_slots.insert(id);
  }

  void clear() {
    m_storage.clear();
    m_free_slots.clear();
  }

  iterator begin() { return iterator(0, &m_storage, &m_free_slots); }

  const_iterator begin() const {
    return const_iterator(0, &m_storage, &m_free_slots);
  }

  iterator end() {
    return iterator(m_storage.size(), &m_storage, &m_free_slots);
  }

  const_iterator end() const {
    return const_iterator(m_storage.size(), &m_storage, &m_free_slots);
  }

  allocator_type get_allocator() const { return m_storage.get_allocator(); }

 private:
  storage_type           m_storage{};
  free_slot_storage_type m_free_slots{};
};

template <typename T, typename Alloc>
template <bool is_const>
class data_storage<T, Alloc>::basic_iterator {
 public:
  using value_type = T;
  using pointer =
      std::conditional_t<is_const, value_type *, const value_type *>;
  using reference =
      std::conditional_t<is_const, const value_type &, value_type &>;
  using difference_type = std::ptrdiff_t;

  basic_iterator() = default;

  basic_iterator(const std::size_t                   init_index,
                 const storage_type *const           storage,
                 const free_slot_storage_type *const free_slot_storage)
      : m_storage(storage),
        m_free_slot_storage(free_slot_storage),
        m_index(init_index) {
    priv_move_to_first_valid_pos();
  }

  bool operator==(const basic_iterator &other) const {
    return m_storage == other.m_storage &&
           m_free_slot_storage == other.m_free_slot_storage &&
           m_index == other.m_index;
  }

  bool operator!=(const basic_iterator &other) const {
    return !(*this == other);
  }

  basic_iterator &operator++() {
    priv_next();
    return *this;
  }

  basic_iterator operator++(int) {
    basic_iterator<is_const> tmp(*this);
    priv_next();
    return tmp;
  }

  pointer operator->() const { return &(ref_storage()[m_index]); }

  reference operator*() const { return ref_storage()[m_index]; }

 private:
  storage_type &ref_storage() const {
    return *(const_cast<storage_type *>(metall::to_raw_pointer(m_storage)));
  }

  void priv_move_to_first_valid_pos() {
    while (m_index < m_storage->size() && m_free_slot_storage->count(m_index)) {
      ++m_index;
    }
  }

  void priv_next() {
    if (m_storage->size() <= m_index) {
      return;
    }
    ++m_index;
    while (m_index < m_storage->size() && m_free_slot_storage->count(m_index)) {
      ++m_index;
    }
  }

  typename std::pointer_traits<void_pointer>::template rebind<
      const storage_type>
      m_storage{nullptr};
  typename std::pointer_traits<void_pointer>::template rebind<
      const free_slot_storage_type>
              m_free_slot_storage{nullptr};
  std::size_t m_index{0};
};

}  // namespace json_bento::jbdtl
