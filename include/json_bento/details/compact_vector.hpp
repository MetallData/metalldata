// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <metall/detail/utilities.hpp>
#include <metall/offset_ptr.hpp>

#include <cassert>
#include <memory>

#include <json_bento/details/bit_operation.hpp>

namespace json_bento::jbdtl {

/// \brief 1D array data structure, designed to use small amount of memory.
/// \tparam T Value type.
/// \tparam Alloc Allocator type.
/// \warning T must not require allocator.
template <typename T, typename Alloc = std::allocator<T>>
class compact_vector {
 public:
  using value_type = T;
  // Use a name different from 'allocator_type' not to trigger STL's allocator
  // related features.
  using data_allocator_type =
      typename std::allocator_traits<Alloc>::template rebind_alloc<value_type>;
  using void_pointer =
      typename std::allocator_traits<data_allocator_type>::void_pointer;
  using const_void_pointer =
      typename std::allocator_traits<data_allocator_type>::const_void_pointer;
  using pointer =
      typename std::pointer_traits<void_pointer>::template rebind<value_type>;
  using const_pointer = typename std::pointer_traits<
      const_void_pointer>::template rebind<value_type>;
  using reference       = value_type &;
  using const_reference = const value_type &;

  using iterator       = pointer;
  using const_iterator = const_pointer;

  compact_vector() {
    priv_update_capacity(0);
    priv_update_size(0);
  }

  ~compact_vector() noexcept {
    assert(size() == 0);
    assert(capacity() == 0);
    assert(!m_data);
  }

  compact_vector(const compact_vector &) = delete;
  compact_vector(compact_vector &&other) noexcept {
    m_capacity_and_size = other.m_capacity_and_size;
    other.priv_update_capacity(0);
    other.priv_update_size(0);
    m_data       = std::move(other.m_data);
    other.m_data = nullptr;
  }

  compact_vector &operator=(const compact_vector &) = delete;
  compact_vector &operator=(compact_vector &&)      = delete;

  reference operator[](const std::size_t index) { return at(index); }

  const_reference operator[](const std::size_t index) const {
    return at(index);
  }

  std::size_t capacity() const { return priv_capacity(); }

  reference at(const std::size_t index) {
    assert(index < size());
    return m_data[index];
  }

  const_reference at(const std::size_t index) const {
    assert(index < size());
    return m_data[index];
  }

  std::size_t size() const { return priv_size(); }

  /// \brief Expand or shrink the size of the vector.
  /// This function will not change the capacity when new_size <= size().
  /// \param new_size New size.
  /// \param allocator Allocator.
  void resize(const std::size_t new_size, data_allocator_type allocator) {
    priv_resize(new_size, allocator);
  }

  void push_back(value_type &&value, data_allocator_type allocator) {
    priv_push_back(std::forward<value_type>(value), allocator);
  }

  /// \brief Destroy all elements and free the memory.
  /// \param allocator Allocator.
  void destroy(data_allocator_type allocator) { priv_destroy(allocator); }

  /// \brief Clear all elements.
  /// This function does not free the memory.
  /// \param allocator Allocator.
  void clear(data_allocator_type allocator) { priv_clear(allocator); }

  /// \brief Shrink the capacity to the size.
  /// \param allocator Allocator.
  void shrink_to_fit(data_allocator_type allocator) {
    priv_shrink_to_fit(allocator);
  }

  reference back() {
    assert(size() > 0);
    return m_data[size() - 1];
  }

  const_reference back() const {
    assert(size() > 0);
    return m_data[size() - 1];
  }

  iterator begin() { return m_data; }

  const_iterator begin() const { return m_data; }

  iterator end() { return m_data + size(); }

  const_iterator end() const { return m_data + size(); }

 private:
  static constexpr uint64_t k_capacity_mask     = 0xFFFF000000000000ULL;
  static constexpr uint64_t k_capacity_mask_lsb = get_lsb(k_capacity_mask);
  static constexpr uint64_t k_size_mask         = 0x0000FFFFFFFFFFFFULL;
  static_assert(k_capacity_mask - ~k_size_mask == 0,
                "Wrong mask values for capacity and size");

  std::size_t priv_size() const { return m_capacity_and_size & k_size_mask; }

  std::size_t priv_capacity() const {
    const auto capacity_data =
        (m_capacity_and_size & k_capacity_mask) >> k_capacity_mask_lsb;
    // capacity is going to be 1 (2^0) when capacity_data is 1.
    return capacity_data == 0 ? 0 : (1ULL << (capacity_data - 1));
  }

  void priv_update_capacity(const std::size_t new_capacity) {
    const auto current_size = m_capacity_and_size & ~0xFFFF000000000000ULL;
    const auto new_capacity_log2 =
        (new_capacity == 0)
            ? 0
            : metall::mtlldetail::log2_dynamic(new_capacity) + 1;
    m_capacity_and_size =
        (new_capacity_log2 << k_capacity_mask_lsb) | current_size;
  }

  void priv_update_size(const std::size_t new_size) {
    m_capacity_and_size = (m_capacity_and_size & k_capacity_mask) | new_size;
  }

  void priv_reserve(const std::size_t new_cap, data_allocator_type allocator) {
    if (new_cap <= capacity()) {
      return;
    }

    // Move items to a new memory region
    const auto new_cap_power2 = metall::mtlldetail::next_power_of_2(new_cap);
    auto       new_data = std::allocator_traits<data_allocator_type>::allocate(
        allocator, new_cap_power2);
    assert(new_data);
    for (std::size_t i = 0; i < this->size(); ++i) {
      new (metall::to_raw_pointer(&new_data[i])) T(std::move(m_data[i]));
    }

    priv_deallocate_data_array(allocator);
    m_data   = new_data;
    new_data = nullptr;
    priv_update_capacity(new_cap_power2);
    // priv_update_size(old_size);
  }

  void priv_resize(const std::size_t new_size, data_allocator_type allocator) {
    if (new_size == size()) {
      return;  // Do nothing.
    } else if (new_size < size()) {
      for (std::size_t i = new_size; i < size(); ++i) {
        priv_destroy_item_at(i, allocator);
      }
      // Does not shrink the capacity.
    } else {
      priv_reserve(new_size, allocator);
      for (std::size_t i = size(); i < new_size; ++i) {
        new (metall::to_raw_pointer(&m_data[i])) T();
      }
    }
    priv_update_size(new_size);
  }

  void priv_shrink_to_fit(data_allocator_type allocator) {
    if (size() == capacity()) {
      return;  // Do nothing.
    }
    assert(size() < capacity());

    if (size() == 0) {
      priv_deallocate_data_array(allocator);
      return;
    }

    // Move items to a new smaller memory region
    // Cannot shrink to the exact size because the capacity must be a power
    // of 2.
    const auto size_power2 = metall::mtlldetail::next_power_of_2(size());
    auto       new_data = std::allocator_traits<data_allocator_type>::allocate(
        allocator, size_power2);
    assert(new_data);
    for (std::size_t i = 0; i < this->size(); ++i) {
      new (metall::to_raw_pointer(&new_data[i])) T(std::move(m_data[i]));
    }

    priv_deallocate_data_array(allocator);
    m_data   = new_data;
    new_data = nullptr;
    priv_update_capacity(size_power2);
  }

  void priv_push_back(value_type &&value, data_allocator_type allocator) {
    resize(size() + 1, allocator);
    new (metall::to_raw_pointer(&back())) value_type(std::move(value));
  }

  void priv_clear(data_allocator_type allocator) {
    priv_destroy_all_items(allocator);
    priv_update_size(0);
  }

  void priv_destroy(data_allocator_type allocator) {
    priv_clear(allocator);
    priv_deallocate_data_array(allocator);
  }

  void priv_destroy_all_items(data_allocator_type allocator) noexcept {
    for (std::size_t i = 0; i < size(); ++i) {
      priv_destroy_item_at(i, allocator);
    }
  }

  void priv_destroy_item_at(const std::size_t   index,
                            data_allocator_type allocator) noexcept {
    std::allocator_traits<data_allocator_type>::destroy(
        allocator, std::addressof(m_data[index]));
  }

  void priv_deallocate_data_array(data_allocator_type allocator) noexcept {
    if (capacity() == 0) {
      assert(!m_data);
      return;
    }

    std::allocator_traits<data_allocator_type>::deallocate(allocator, m_data,
                                                           capacity());
    m_data = nullptr;
    priv_update_capacity(0);
  }

  pointer  m_data{nullptr};
  uint64_t m_capacity_and_size{0};
};

}  // namespace json_bento::jbdtl
