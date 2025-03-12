// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <memory>
#include <scoped_allocator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include <boost/container/deque.hpp>
#include <boost/container/string.hpp>
#include <boost/container/vector.hpp>
#include <boost/unordered/unordered_map.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

// Experimental feature
#ifndef METALLDATA_MSR_DEQUE_BLOCK_SIZE
#define METALLDATA_MSR_DEQUE_BLOCK_SIZE (1024UL * 1024 * 2)
#endif

namespace multiseries {

namespace {
namespace bc = boost::container;
}  // namespace

enum class container_kind { dense, sparse };

template <typename Value, typename Alloc = std::allocator<Value>>
class series_container {
 public:
  using value_type     = Value;
  using allocator_type = Alloc;

 private:
  template <typename T>
  using other_allocator =
      typename std::allocator_traits<Alloc>::template rebind_alloc<T>;

  template <typename T>
  using scp_allocator = std::scoped_allocator_adaptor<other_allocator<T>>;

  using pointer_type = typename std::allocator_traits<Alloc>::pointer;

  struct value_with_flag {
    bool       empty{true};
    value_type value;
  };

  //  template <typename T>
  //  using vector_type = bc::vector<T, scp_allocator<T>>;

  using deque_block_option_t =
      bc::deque_options<bc::block_bytes<METALLDATA_MSR_DEQUE_BLOCK_SIZE>>::type;
  template <typename T>
  using deque_type = bc::deque<T, scp_allocator<T>, deque_block_option_t>;

  template <typename T>
  using map_type =
      boost::unordered_flat_map<size_t, T, std::hash<size_t>,
                                std::equal_to<size_t>,
                                scp_allocator<std::pair<const size_t, T>>>;

 public:

  explicit series_container(const allocator_type &alloc = allocator_type())
      : m_map_container(alloc), m_deq_container(alloc) {}

  explicit series_container(const container_kind &kind,
                            const allocator_type &alloc = allocator_type())
      : m_kind(kind), m_map_container(alloc), m_deq_container(alloc) {}

  // Copy constructor
  series_container(const series_container &other) = default;

  // Move constructor
  series_container(series_container &&other) noexcept
      : m_kind(other.m_kind),
        m_n_items(other.m_n_items),
        m_map_container(std::move(other.m_map_container)),
        m_deq_container(std::move(other.m_deq_container)) {
    other.clear();
  }

  // Copy assignment
  series_container &operator=(const series_container &other) = default;

  // Move assignment
  series_container &operator=(series_container &&other) noexcept {
    if (this != &other) {
      m_kind          = other.m_kind;
      m_n_items       = other.m_n_items;
      m_map_container = std::move(other.m_map_container);
      m_deq_container = std::move(other.m_deq_container);
      other.clear();
    }
    return *this;
  }

  // Destructor
  ~series_container() noexcept = default;

  // Copy constructor with allocator
  series_container(const series_container &other, const allocator_type &alloc)
      : m_kind(other.m_kind),
        m_n_items(other.m_n_items),
        m_map_container(other.m_map_container, alloc),
        m_deq_container(other.m_deq_container, alloc) {}

  // Move constructor with allocator
  series_container(series_container &&other, const allocator_type &alloc)
      : m_kind(other.m_kind),
        m_n_items(other.m_n_items),
        m_map_container(std::move(other.m_map_container), alloc),
        m_deq_container(std::move(other.m_deq_container), alloc) {
    other.clear();
  }

  // Access the value associated with 'i'
  // Works like the '[]' operator in map container, i.e., if the key does not
  // exist, it creates a new entry. Thus, the slot is not empty anymore.
  value_type &operator[](size_t i) {
    if (m_kind == container_kind::sparse) {
      return m_map_container[i];
    } else if (m_kind == container_kind::dense) {
      if (i >= m_deq_container.size()) {
        m_deq_container.resize(i + 1);
        m_n_items = m_n_items + 1;
      }
      m_deq_container[i].empty = false;
      return m_deq_container[i].value;
    }
    throw std::runtime_error("Unknown container kind");
  }

  const value_type &at(size_t i) const {
    if (m_kind == container_kind::sparse) {
      if (!m_map_container.contains(i)) {
        throw std::out_of_range("Index out of range");
      }
      return m_map_container.at(i);
    } else if (m_kind == container_kind::dense) {
      if (i >= m_deq_container.size()) {
        throw std::out_of_range("Index out of range");
      }
      if (m_deq_container[i].empty) {
        throw "Does not contain a value at the index";
      }
      return m_deq_container[i].value;
    }
    throw std::runtime_error("Unknown container kind");
  }

  size_t size() const {
    if (m_kind == container_kind::sparse) {
      return m_map_container.size();
    } else if (m_kind == container_kind::dense) {
      return m_n_items;
    }
    throw std::runtime_error("Unknown container kind");
  }

  size_t capacity() const {
    if (m_kind == container_kind::sparse) {
      return m_map_container.size();
    } else if (m_kind == container_kind::dense) {
      return m_deq_container.size();
    }
    throw std::runtime_error("Unknown container kind");
  }

  double load_factor() const {
    if (m_kind == container_kind::sparse) {
      return 1.0;
    } else if (m_kind == container_kind::dense) {
      return static_cast<double>(m_n_items) / m_deq_container.size();
    }
    throw std::runtime_error("Unknown container kind");
  }

  bool empty() const {
    if (m_kind == container_kind::sparse) {
      return m_map_container.empty();
    } else if (m_kind == container_kind::dense) {
      return m_n_items == 0;
    }
    throw std::runtime_error("Unknown container kind");
  }

  // Returns true if the value associated with 'i' is empty
  bool contains(size_t i) const {
    if (m_kind == container_kind::sparse) {
      return m_map_container.contains(i);
    } else if (m_kind == container_kind::dense) {
      if (i >= m_deq_container.size()) {
        return false;
      }
      return !m_deq_container[i].empty;
    }
    throw std::runtime_error("Unknown container kind");
  }

  void clear() {
    m_map_container.clear();
    m_deq_container.clear();
    m_n_items = 0;
  }

  bool erase(size_t i) {
    if (m_kind == container_kind::sparse) {
      return m_map_container.erase(i) > 0;
    } else if (m_kind == container_kind::dense) {
      if (i >= m_deq_container.size()) {
        return false;
      }
      if (!m_deq_container[i].empty) {
        m_deq_container[i].value.~value_type();
        m_deq_container[i].empty = true;
        --m_n_items;
        return true;
      }
      return false;
    }
    throw std::runtime_error("Unknown container kind");
  }

  container_kind kind() const { return m_kind; }

  // Move value to the new container kind
  void convert(const container_kind &new_kind) {
    if (m_kind == new_kind) {
      return;
    }

    if (new_kind == container_kind::sparse) {
      // Convert to sparse
      for (size_t i = 0; i < m_deq_container.size(); ++i) {
        if (!m_deq_container[i].empty) {
          m_map_container[i] = std::move(m_deq_container[i].value);
        }
        m_n_items = 0;
      }
      m_deq_container.clear();
      m_n_items = 0;
    } else if (new_kind == container_kind::dense) {
      // Convert to dense
      size_t max_index = 0;
      for (const auto &pair : m_map_container) {
        max_index = std::max(max_index, pair.first);
      }
      const auto new_dense_size = max_index + 1;
      m_deq_container.resize(new_dense_size);
      m_n_items = 0;
      for (auto &pair : m_map_container) {
        m_deq_container[pair.first].empty = false;
        m_deq_container[pair.first].value = std::move(pair.second);
        ++m_n_items;
      }
      m_map_container.clear();
    } else {
      throw std::runtime_error("Unknown container kind");
    }

    m_kind = new_kind;
  }

 private:
  container_kind m_kind{container_kind::dense};
  size_t         m_n_items{0};  // Used only for the dense container
  deque_type<value_with_flag> m_deq_container;
  map_type<value_type>        m_map_container;
};
}  // namespace multiseries