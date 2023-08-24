// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <metall/detail/utilities.hpp>
#include <metall/offset_ptr.hpp>

#include <cassert>
#include <memory>
#include <utility>

namespace json_bento::jbdtl {

template <typename T, typename Allocator = std::allocator<T>>
class vector {
  static_assert(
      std::is_same_v<T, typename std::allocator_traits<Allocator>::value_type>,
      "Different allocate value type");

 public:
  using value_type      = T;
  using allocator_type  = Allocator;
  using size_type       = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference       = value_type&;
  using const_reference = const value_type&;
  using pointer         = typename std::allocator_traits<Allocator>::pointer;
  using const_pointer =
      typename std::allocator_traits<Allocator>::const_pointer;
  using iterator       = value_type*;
  using const_iterator = const value_type*;

  explicit vector(const allocator_type& alloc = allocator_type())
      : m_allocator(alloc) {}

  vector(const size_type       capacity,
         const allocator_type& alloc = allocator_type())
      : m_allocator(alloc) {
    std::tie(m_storage, m_capacity) = priv_allocate(capacity);
  }

  ~vector() noexcept {
    priv_clear();
    priv_deallocate(m_storage, m_capacity);
  }

  void swap(vector& other) {
    using std::swap;
    swap(m_allocator, other.m_allocator);
    swap(m_storage, other.m_storage);
    swap(m_capacity, other.m_capacity);
    swap(m_size, other.m_size);
  }

  size_type size() const noexcept { return m_size; }

  size_type capacity() const noexcept { return m_capacity; }

  void clear() { priv_clear(); }

  reference operator[](const size_t pos) {
    assert(pos < m_size);
    return m_storage[pos];
  }

  const_reference operator[](const size_t pos) const {
    assert(pos < m_size);
    return m_storage[pos];
  }

  reference at(const size_t pos) {
    assert(pos < m_size);
    return m_storage[pos];
  }

  const_reference at(const size_t pos) const {
    assert(pos < m_size);
    return m_storage[pos];
  }

  reference front() { return m_storage[0]; }

  reference front() const { return m_storage[0]; }

  reference back() { return m_storage[size() - 1]; }

  reference back() const { return m_storage[size() - 1]; }

  iterator begin() noexcept { return m_storage; }

  const_iterator begin() const noexcept { return m_storage; }

  iterator end() noexcept { return m_storage + m_size; }

  const_iterator end() const noexcept { return m_storage + m_size; }

  template <typename... Args>
  reference emplace_back(Args&&... args) {
    return priv_emplace_back(std::forward<Args>(args)...);
  }

  allocator_type get_allocator() const { return m_allocator; }

 private:
  std::pair<pointer, size_type> priv_allocate(const size_type capacity) {
    const auto power2_capacity = metall::mtlldetail::next_power_of_2(capacity);

    pointer ptr = std::allocator_traits<allocator_type>::allocate(
        m_allocator, power2_capacity);
    if (!ptr) {
      std::cerr << "Failed to allocate " << power2_capacity << std::endl;
      std::abort();
    }
    return std::make_pair(ptr, power2_capacity);
  }

  void priv_deallocate(pointer buf, const std::size_t capacity) noexcept {
    std::allocator_traits<allocator_type>::deallocate(m_allocator, buf,
                                                      capacity);
  }

  void priv_clear() {
    for (size_type i = 0; i < m_size; ++i) {
      std::allocator_traits<allocator_type>::destroy(m_allocator,
                                                     &m_storage[i]);
    }
    m_size = 0;
  }

  void priv_extend(const std::size_t new_capacity_request) {
    if (m_capacity >= new_capacity_request) return;

    pointer   new_storage;
    size_type new_capacity;
    std::tie(new_storage, new_capacity) = priv_allocate(new_capacity_request);

    for (size_type i = 0; i < m_size; ++i) {
      new_storage[i] = std::move(m_storage[i]);
    }

    priv_deallocate(m_storage, m_capacity);

    m_capacity = new_capacity;
    m_storage  = new_storage;
  }

  template <typename... Args>
  reference priv_emplace_back(Args&&... args) {
    if (size() + 1 > capacity()) {
      priv_extend(size() == 0 ? 2 : size() * 2);
    }
    new (metall::to_raw_pointer(m_storage + size()))
        value_type(std::forward<Args>(args)...);
    ++m_size;
    return back();
  }

  allocator_type m_allocator{};
  pointer        m_storage{nullptr};
  size_type      m_capacity{0};
  size_type      m_size{0};
};

}  // namespace json_bento::jbdtl
