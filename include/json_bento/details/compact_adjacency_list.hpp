// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cassert>
#include <memory>

#include <json_bento/details/compact_vector.hpp>

namespace json_bento::jbdtl {

/// \warning T must not require allocator.
template <typename T, typename Alloc>
class compact_adjacency_list {
 private:
  using row_list_type          = compact_vector<T, Alloc>;
  using column_list_alloc_type = typename std::allocator_traits<
      Alloc>::template rebind_alloc<row_list_type>;
  using column_list_type =
      compact_vector<row_list_type, column_list_alloc_type>;

 public:
  using value_type     = T;
  using allocator_type = Alloc;
  using void_pointer =
      typename std::allocator_traits<allocator_type>::void_pointer;
  using const_void_pointer =
      typename std::allocator_traits<allocator_type>::const_void_pointer;
  using pointer =
      typename std::pointer_traits<void_pointer>::template rebind<T>;
  using const_pointer =
      typename std::pointer_traits<const_void_pointer>::template rebind<T>;
  using reference             = T &;
  using const_reference       = const value_type &;
  using column_iterator       = typename column_list_type::iterator;
  using const_column_iterator = typename column_list_type::const_iterator;
  using row_iterator          = typename row_list_type::iterator;
  using const_row_iterator    = typename row_list_type::const_iterator;

  compact_adjacency_list(){};

  explicit compact_adjacency_list(const allocator_type &alloc)
      : m_allocator(alloc) {}

  ~compact_adjacency_list() noexcept { priv_destroy(); }

  compact_adjacency_list(const compact_adjacency_list &)     = default;
  compact_adjacency_list(compact_adjacency_list &&) noexcept = default;

  compact_adjacency_list &operator=(const compact_adjacency_list &) = default;
  compact_adjacency_list &operator=(compact_adjacency_list &&) noexcept =
      default;

  reference at(const std::size_t row, const std::size_t col) {
    return m_table.at(row).at(col);
  }

  const_reference at(const std::size_t row, const std::size_t col) const {
    return m_table.at(row).at(col);
  }

  reference back(const std::size_t row) { return m_table.at(row).back(); }

  const_reference back(const std::size_t row) const {
    return m_table.at(row).back();
  }

  column_iterator begin() { return m_table.begin(); }

  const_column_iterator begin() const { return m_table.begin(); }

  column_iterator end() { return m_table.end(); }

  const_column_iterator end() const { return m_table.end(); }

  row_iterator begin(const std::size_t row) { return m_table.at(row).begin(); }

  const_row_iterator begin(const std::size_t row) const {
    return m_table.at(row).begin();
  }

  row_iterator end(const std::size_t row) { return m_table.at(row).end(); }

  const_row_iterator end(const std::size_t row) const {
    return m_table.at(row).end();
  }

  void resize(const std::size_t size) { priv_resize(size); }

  void resize(const std::size_t row, const std::size_t size) {
    m_table.at(row).resize(size, m_allocator);
  }

  std::size_t push_back() {
    m_table.push_back(row_list_type(), m_allocator);
    return m_table.size() - 1;
  }

  std::size_t push_back(const std::size_t row, value_type &&value) {
    if (row >= size()) {
      resize(row + 1);
    }
    m_table.at(row).push_back(std::forward<value_type>(value), m_allocator);
    return m_table.at(row).size() - 1;
  }

  std::size_t size() const { return m_table.size(); }

  std::size_t size(const std::size_t row) const {
    if (row >= size()) {
      return 0;
    }
    return m_table.at(row).size();
  }

  std::size_t capacity() const { return m_table.capacity(); }

  std::size_t capacity(const std::size_t row) const {
    if (row >= size()) {
      return 0;
    }
    return m_table.at(row).capacity();
  }

  /// \brief Clear the row.
  /// \warning This function does not shrink the memory of the row.
  void clear(const std::size_t row) { m_table.at(row).clear(m_allocator); }

  /// \brief Clear all rows.
  /// \warning This function shrink the memory of each row but not that of main
  /// table.
  void clear() {
    for (auto &item : m_table) {
      item.destroy(m_allocator);
    }
    m_table.clear(m_allocator);
  }

  /// \brief Shrink the memory of the row.
  void shrink_to_fit(const std::size_t row) {
    m_table.at(row).shrink_to_fit(m_allocator);
  }

  /// \brief Shrink the memory.
  void shrink_to_fit() {
    for (auto &item : m_table) {
      item.shrink_to_fit(m_allocator);
    }
    m_table.shrink_to_fit(m_allocator);
  }

 private:
  void priv_destroy() {
    for (std::size_t i = 0; i < m_table.size(); ++i) {
      m_table.at(i).destroy(m_allocator);
    }
    m_table.destroy(m_allocator);
  }

  void priv_resize(const std::size_t size) {
    if (size == m_table.size()) {
      return;
    }

    if (size < m_table.size()) {
      for (std::size_t i = size; i < m_table.size(); ++i) {
        m_table.at(i).destroy(m_allocator);
      }
    }

    m_table.resize(size, m_allocator);
  }

  allocator_type   m_allocator{allocator_type{}};
  column_list_type m_table{};
};

}  // namespace json_bento::jbdtl
