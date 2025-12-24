// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <boost/container/vector.hpp>

#include <string_table/string_store.hpp>

namespace compact_string {
template <typename Alloc = std::allocator<std::byte>>
class vector {
 private:
  using self_type = vector<Alloc>;

  template <typename T>
  using other_allocator =
    typename std::allocator_traits<Alloc>::template rebind_alloc<T>;

  template <typename T>
  using other_scoped_allocator =
    std::scoped_allocator_adaptor<other_allocator<T>>;

  using pointer_type = typename std::allocator_traits<Alloc>::pointer;

  using internal_value_type = string_accessor;
  using internal_vector_type =
    boost::container::vector<internal_value_type,
                             other_allocator<internal_value_type>>;

 public:
  using char_type      = char;
  using allocator_type = Alloc;
  using const_iterator = typename internal_vector_type::const_iterator;

  using string_store_type = string_store<Alloc>;

  vector() = default;

  explicit vector(string_store_type *const string_table, const Alloc &alloc)
      : m_vector(alloc), string_table(string_table) {}

  vector(const vector &)                = default;
  vector(vector &&) noexcept            = default;
  vector &operator=(const vector &)     = default;
  vector &operator=(vector &&) noexcept = default;
  ~vector()                             = default;

  std::string_view operator[](const size_t i) const {
    return m_vector[i].to_view();
  }

  std::string_view at(const size_t i) const { return m_vector.at(i).to_view(); }

  void push_back(std::string_view str) {
    m_vector.push_back(add_string(str.data(), str.length(), *string_table));
  }

  /// FIXME: this is a temporary solution to update data
  void assign(std::string_view str, const size_t i) {
    m_vector[i] = add_string(str, *string_table);
  }

  size_t size() const { return m_vector.size(); }

  void resize(const size_t n) { m_vector.resize(n); }

  void clear() { m_vector.clear(); }

  const_iterator begin() const { return m_vector.begin(); }
  const_iterator end() const { return m_vector.end(); }

  allocator_type get_allocator() const { return m_vector.get_allocator(); }

 private:
  using string_store_pointer_type = typename std::pointer_traits<
    pointer_type>::template rebind<string_store_type>;

  internal_vector_type      m_vector{};
  string_store_pointer_type string_table{nullptr};
};

}  // namespace compact_string