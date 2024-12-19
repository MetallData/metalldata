// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

struct internal_string_type {
  internal_string_type() = default;
  internal_string_type(const char_type *data, const size_t length,
                       char_allocator_type allocator)
      : m_length(length) {
    priv_assign(data, length, allocator);
  }

  ~internal_string_type() {
    assert(!m_data);  // check memory leak
  }

  // Disable copy constructor and copy assignment
  internal_string_type(const internal_string_type &)            = delete;
  internal_string_type &operator=(const internal_string_type &) = delete;

  // Move constructor
  internal_string_type(internal_string_type &&other) noexcept
      : m_data(other.m_data), m_length(other.m_length) {
    other.m_data   = nullptr;
    other.m_length = 0;
  }

  // Move assignment
  internal_string_type &operator=(internal_string_type &&other) = delete;

  // Swap
  void swap(internal_string_type &other) {
    std::swap(m_data, other.m_data);
    std::swap(m_length, other.m_length);
  }

  // equal operator
  bool operator==(const internal_string_type &other) const {
    if (m_length != other.m_length) {
      return false;
    }
    return std::equal(m_data, m_data + m_length, other.m_data);
  }

  bool operator!=(const internal_string_type &other) const {
    return !(*this == other);
  }

  const char_type *c_str() const { return std::to_address(m_data); }

  size_t size() const { return m_length; }

  size_t length() const { return m_length; }

  const char_type *data() const { return std::to_address(m_data); }

  void assign(const char_type *data, const size_t length,
              char_allocator_type allocator) {
    priv_assign(data, length, allocator);
  }

  void destroy(char_allocator_type allocator) {
    if (m_data) {
      std::allocator_traits<char_allocator_type>::deallocate(
          allocator, std::to_address(m_data), m_length + 1);
      m_data   = nullptr;
      m_length = 0;
    }
  }

 private:
  void priv_assign(const char_type *data, const size_t length,
                   char_allocator_type allocator) {
    if (m_data) {
      std::allocator_traits<char_allocator_type>::deallocate(
          allocator, std::to_address(m_data), m_length + 1);
    }
    m_length = length;
    if (length == 0) {
      m_data = nullptr;
      return;
    }
    m_data = std::allocator_traits<char_allocator_type>::allocate(allocator,
                                                                  length + 1);
    std::copy(data, data + length, m_data);
    m_data[length] = '\0';
  }

  char_pointer_type m_data{nullptr};
  size_t            m_length{0};
};

struct internal_string_hash {
  std::size_t operator()(const internal_string_type &str) const {
    return boost::hash_range(str.data(), str.data() + str.length());
  }
};
