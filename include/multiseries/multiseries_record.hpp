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

#include "string_table/string_store.hpp"
#include "multiseries/container.hpp"

namespace multiseries {
namespace {
namespace bc   = boost::container;
namespace cstr = compact_string;
}  // namespace

/// \brief Column-based record store
/// \tparam Alloc The type of the allocator
/// \details
/// This class provides a column-based record store.
/// Each record (row) can have multiple series (columns).
/// Each series can have different types. Supported types are int64_t, uint64_t,
/// double, and std::string_view.
template <typename Alloc = std::allocator<std::byte>>
class basic_record_store {
 private:
  template <typename T>
  using other_allocator =
    typename std::allocator_traits<Alloc>::template rebind_alloc<T>;

  template <typename T>
  using scp_allocator = std::scoped_allocator_adaptor<other_allocator<T>>;

  using pointer_type = typename std::allocator_traits<Alloc>::pointer;

  template <typename T>
  using other_pointer_type =
    typename std::pointer_traits<pointer_type>::template rebind<T>;

 public:
  using series_index_type         = size_t;
  using record_id_type            = size_t;
  using allocator_type            = Alloc;
  using string_store_type         = cstr::string_store<allocator_type>;
  using string_store_pointer_type = other_pointer_type<string_store_type>;
  using series_type = std::variant<std::monostate, bool, int64_t, uint64_t,
                                   double, std::string_view>;

 private:
  template <typename T>
  using vector_type = bc::vector<T, scp_allocator<T>>;

  using deque_block_option_t =
    bc::deque_options<bc::block_bytes<METALLDATA_MSR_DEQUE_BLOCK_SIZE>>::type;
  template <typename T>
  using deque_type = bc::deque<T, scp_allocator<T>, deque_block_option_t>;

  template <typename T>
  using sparce_map_type =
    boost::unordered_flat_map<size_t, T, std::hash<size_t>,
                              std::equal_to<size_t>,
                              scp_allocator<std::pair<const size_t, T>>>;

  // If T is std::string_view, use cstr::string_accessor
  // Otherwise, use T
  template <typename T>
  using series_stored_type =
    std::conditional_t<std::is_same_v<T, std::string_view>,
                       cstr::string_accessor, T>;

  // Container to store a single series
  template <typename T>
  using series_container_type = series_container<series_stored_type<T>, Alloc>;

  using container_variant =
    std::variant<series_container_type<bool>, series_container_type<int64_t>,
                 series_container_type<uint64_t>, series_container_type<double>,
                 series_container_type<std::string_view>>;

  using string_type =
    bc::basic_string<char, std::char_traits<char>, other_allocator<char>>;

  struct series_header {
    string_type       name;
    container_variant container;
  };
  using multiseries_main_container_type = vector_type<series_header>;

 public:
  explicit basic_record_store(string_store_type    *string_store,
                              const allocator_type &alloc = allocator_type())
      : m_record_status(alloc), m_series(alloc), m_string_store(string_store) {}

  record_id_type add_record() {
    // Does not increment container capacity yet
    m_record_status.push_back(true);
    return m_record_status.size() - 1;
  }

  /// \brief Returns the maximum record index.
  record_id_type max_index() { return m_record_status.size() - 1; }

  /// \brief Add a series
  /// \param series_name The name of the series
  /// \param kind The kind of the container
  template <typename series_type>
  series_index_type add_series(const std::string_view series_name,
                               container_kind kind = container_kind::dense) {
    priv_series_type_check<series_type>();

    // Check if the series already exists
    auto itr = priv_find_series(series_name);
    if (itr != m_series.end()) {
      return (size_t)std::abs(std::distance(m_series.begin(), itr));
    }

    m_series.push_back(
      {.name      = string_type(series_name.data(), series_name.size(),
                                m_record_status.get_allocator()),
       .container = series_container_type<series_type>(
         kind, m_record_status.get_allocator())});

    return m_series.size() - 1;
  }

  /// \brief Returns the series data of a record
  /// \param series_name The name of the series
  /// \param record_id The record ID
  /// \return The series data
  /// If the series data does not exist, it throws a runtime error.
  template <typename series_type>
  const auto get(const std::string_view series_name,
                 const record_id_type   record_id) const {
    priv_series_type_check<series_type>();
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found: " + std::string(series_name));
    }
    return priv_get_series_data<series_type>(itr->container, record_id);
  }

  /// \brief Returns the series data of a record
  /// \param series_index The index of the series
  /// \param record_id The record ID
  /// \return The series data
  /// If the series data does not exist, it throws a runtime error.
  template <typename series_type>
  const auto get(const series_index_type series_index,
                 const record_id_type    record_id) const {
    priv_series_type_check<series_type>();
    if (series_index >= m_series.size()) {
      throw std::runtime_error("Series not found");
    }
    const auto &container = m_series[series_index].container;
    return priv_get_series_data<series_type>(container, record_id);
  }

  /// \brief Returns all series data of a single record
  ///\param record_id Record ID
  ///\return A vector of std::variant containing all series data
  std::vector<series_type> get(const record_id_type record_id) const {
    if (!contains_record(record_id)) {
      return {};
    }

    std::vector<series_type> to_return_record(m_series.size(),
                                              std::monostate{});
    for (size_t si = 0; si < m_series.size(); ++si) {
      std::visit(
        [&to_return_record, record_id, si](const auto &container) {
          if (!container.contains(record_id)) return;
          using T = std::decay_t<decltype(container)>;
          if constexpr (std::is_same_v<
                          T, series_container_type<std::string_view>>) {
            to_return_record[si] = container.at(record_id).to_view();
          } else {
            to_return_record[si] = container.at(record_id);
          }
        },
        m_series[si].container);
    }
    return to_return_record;
  }

  /// \brief Returns if a series data of a record is None (does not exist)
  bool is_none(const std::string_view series_name,
               const record_id_type   record_id) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      return true;
    }

    return !std::visit(
      [&record_id](const auto &container) {
        return container.contains(record_id);
      },
      itr->container);
  }

  /// \brief Returns if a series data of a record is None (does not exist)
  bool is_none(const series_index_type series_index,
               const record_id_type    record_id) const {
    if (series_index >= m_series.size()) {
      return true;
    }
    return !std::visit(
      [&record_id](const auto &container) {
        return container.contains(record_id);
      },
      m_series[series_index].container);
  }

  /// \brief Set a series data of a record (row)
  template <typename series_type>
  void set(const std::string_view series_name, const record_id_type record_id,
           series_type value) {
    priv_series_type_check<series_type>();
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found: " + std::string(series_name));
    }

    priv_set_series_data<series_type>(*itr, record_id, value);
  }

  template <typename series_type>
  void set(const series_index_type series_index, const record_id_type record_id,
           series_type value) {
    priv_series_type_check<series_type>();
    if (series_index >= m_series.size()) {
      throw std::runtime_error("Series not found");
    }

    priv_set_series_data<series_type>(m_series[series_index], record_id, value);
  }

  template <typename series_type>
  series_index_type find_series(const std::string_view series_name) const {
    priv_series_type_check<series_type>();
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      return std::numeric_limits<size_t>::max();
    }
    return std::abs(std::distance(m_series.begin(), itr));
  }

  //// Returns the number of records (rows)
  size_t num_records() const {
    return std::count(m_record_status.begin(), m_record_status.end(), true);
  }

  /// \brief Returns the number of series (columns)
  size_t num_series() const { return m_series.size(); }

  /// \brief Returns the number of non-None items in a series
  size_t size(const std::string_view series_name) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found: " + std::string(series_name));
    }

    return std::visit(
      [this](const auto &container) { return container.size(); },
      itr->container);
  }

  /// \brief Loop over all records of a series, skipping None values.
  /// series_func_t: [](int record_id, auto single_series_value) {}
  template <typename series_type, typename series_func_t>
  void for_all(const std::string_view series_name,
               series_func_t          series_func) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found: " + std::string(series_name));
    }

    const auto &container =
      priv_get_series_container<series_type>(itr->container);
    for (size_t i = 0; i < m_record_status.size(); ++i) {
      if (m_record_status[i] && container.contains(i)) {
        series_func(i, container.at(i));
      }
    }
  }

  template <typename series_type, typename series_func_t>
  void for_all(const series_index_type series_index,
               series_func_t           series_func) const {
    if (series_index >= m_series.size()) {
      throw std::runtime_error("Series not found");
    }

    const auto &container =
      priv_get_series_container<series_type>(m_series[series_index]);
    for (size_t i = 0; i < m_record_status.size(); ++i) {
      if (m_record_status[i] && container.contains(i)) {
        series_func(i, container.at(i));
      }
    }
  }

  // Change name
  template <typename series_func_t>
  void visit_field(const std::string_view series_name,
                   const record_id_type   record_id,
                   series_func_t          series_func) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found: " + std::string(series_name));
    }
    if (!is_record_valid(record_id)) {
      throw std::runtime_error("Invalid record");
    }

    const auto &series_item = *itr;

    std::visit(
      [&series_func, record_id](const auto &container) {
        if (!container.contains(record_id)) return;
        using T = std::decay_t<decltype(container)>;
        if constexpr (std::is_same_v<T,
                                     series_container_type<std::string_view>>) {
          series_func(container.at(record_id).to_view());
        } else {
          series_func(container.at(record_id));
        }
      },
      series_item.container);
  }

  /// \brief for_all() without series_type
  template <typename series_func_t>
  void for_all_dynamic(const std::string_view series_name,
                       series_func_t          series_func) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found: " + std::string(series_name));
    }

    const auto &series_item = *itr;
    for (size_t i = 0; i < m_record_status.size(); ++i) {
      if (!m_record_status[i]) {  // Just for performance optimization
        continue;
      }
      std::visit(
        [&series_func, i](const auto &container) {
          if (!container.contains(i)) return;
          using T = std::decay_t<decltype(container)>;
          if constexpr (std::is_same_v<
                          T, series_container_type<std::string_view>>) {
            series_func(i, container.at(i).to_view());
          } else {
            series_func(i, container.at(i));
          }
        },
        series_item.container);
    }
  }

  /// \brief for_all() across all series.
  /// Fn takes the row index and a vector of series_type. std::monostate
  /// indicates a missing value.
  template <typename Fn>
  void for_all_dynamic(Fn func) const {
    for (record_id_type i = 0; i < m_record_status.size(); ++i) {
      auto row = get(i);
      func(i, row);
    }
  }
  /// \brief Returns if a series exists associated with the name
  bool contains_series(const std::string_view series_name) const {
    return priv_find_series(series_name) != m_series.end();
  }

  bool contains_series(const series_index_type index) const {
    return index < m_series.size();
  }

  /// \brief Returns if a series exists associated with the name
  bool contains_record(const record_id_type id) const {
    return m_record_status.size() > id && m_record_status[id];
  }

  /// \brief Returns the series names
  std::vector<std::string> get_series_names() const {
    std::vector<std::string> series_names;
    for (const auto &item : m_series) {
      series_names.push_back(item.name);
    }
    return series_names;
  }

  /// \brief Remove a single data
  bool remove(const std::string_view series_name,
              const record_id_type   record_id) {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      return false;
    }

    bool to_return = false;
    std::visit(
      [&record_id, &to_return](auto &container) {
        if (container.contains(record_id)) {
          to_return = container.erase(record_id);
        }
      },
      itr->container);
    return to_return;
  }

  /// \brief Remove a single data
  bool remove(const series_index_type series_index,
              const record_id_type    record_id) {
    if (series_index >= m_series.size()) {
      return false;
    }

    bool to_return = false;
    std::visit(
      [&record_id, &to_return](auto &container) {
        if (container.contains(record_id)) {
          to_return = container.erase(record_id);
        }
      },
      m_series[series_index].container);
    return to_return;
  }

  /// \brief Remove a series by name
  bool remove_series(const std::string_view series_name) {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      return false;
    }

    m_series.erase(itr);
    return true;
  }

  bool remove_series(const series_index_type series_index) {
    return m_series.erase(m_series.begin() + series_index) != m_series.end();
  }

  /// \brief Remove a record, destroy all series data of the record
  bool remove_record(const record_id_type record_id) {
    if (record_id >= m_record_status.size()) {
      return false;
    }

    for (auto &series : m_series) {
      std::visit([&record_id](auto &container) { container.erase(record_id); },
                 series.container);
    }

    m_record_status[record_id] = false;
    return true;
  }

  /// \brief Check if the series is of a specific type.
  /// Returns false if the series does not exist.
  template <typename series_type>
  bool is_series_type(const std::string_view series_name) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      return false;
    }
    return std::holds_alternative<series_container_type<series_type>>(
      itr->container);
  }

  template <typename series_type>
  bool is_series_type(const series_index_type series_index) const {
    if (series_index >= m_series.size()) {
      return false;
    }
    return std::holds_alternative<series_container_type<series_type>>(
      m_series[series_index].container);
  }

  /// \Note: this function is deprecated and replaced by contains_record()
  bool is_record_valid(size_t record_index) const {
    return contains_record(record_index);
  }

  /// \brief Convert the container kind of a series
  void convert(const std::string_view series_name, container_kind new_kind) {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found: " + std::string(series_name));
    }

    std::visit([new_kind](auto &container) { container.convert(new_kind); },
               itr->container);
  }

  void convert(const series_index_type series_index, container_kind new_kind) {
    if (series_index >= m_series.size()) {
      throw std::runtime_error("Series not found");
    }

    std::visit([new_kind](auto &container) { container.convert(new_kind); },
               m_series[series_index].container);
  }

  /// \brief Returns the load factor of the series, which is determined by
  /// load-factor = non-None items / #of records.
  /// This one does not consider
  double load_factor(const std::string_view series_name) const {
    return double(size(series_name)) / m_record_status.size();
  }

 private:
  template <class series_type>
  static constexpr void priv_series_type_check() {
    static_assert(std::is_same_v<series_type, bool> ||
                    std::is_same_v<series_type, int64_t> ||
                    std::is_same_v<series_type, uint64_t> ||
                    std::is_same_v<series_type, double> ||
                    std::is_same_v<series_type, std::string_view>,
                  "Unsupported series type");
  }

  multiseries_main_container_type::iterator priv_find_series(
    const std::string_view series_name) {
    for (auto itr = m_series.begin(); itr != m_series.end(); ++itr) {
      if (itr->name == series_name) {
        return itr;
      }
    }
    return m_series.end();
  }

  multiseries_main_container_type::const_iterator priv_find_series(
    const std::string_view series_name) const {
    for (auto itr = m_series.cbegin(); itr != m_series.cend(); ++itr) {
      if (itr->name == series_name) {
        return itr;
      }
    }
    return m_series.cend();
  }

  template <typename series_type>
  const auto &priv_get_series_container(
    const container_variant &series_store) const {
    return std::get<series_container_type<series_type>>(series_store);
  }

  template <typename series_type>
  auto &priv_get_series_container(container_variant &series_store) {
    return std::get<series_container_type<series_type>>(series_store);
  }

  template <typename series_type>
  const auto priv_get_series_data(const container_variant &series_store,
                                  const record_id_type     record_id) const {
    if constexpr (std::is_same_v<series_type, std::string_view>) {
      if (!priv_get_series_container<series_type>(series_store)
             .contains(record_id)) {
        throw std::runtime_error("Series data not found");
      }
      // Returns a string_view (not reference)
      return priv_get_series_container<series_type>(series_store)
        .at(record_id)
        .to_view();
    } else {
      return priv_get_series_container<series_type>(series_store).at(record_id);
    }
  }

  template <typename series_type>
  void priv_set_series_data(series_header       &series,
                            const record_id_type record_id,
                            const series_type   &value) {
    if constexpr (std::is_same_v<series_type, std::string_view>) {
      auto accessor =
        cstr::add_string(value.data(), value.size(), *m_string_store);
      priv_get_series_container<series_type>(series.container)[record_id] =
        accessor;
    } else {
      priv_get_series_container<series_type>(series.container)[record_id] =
        value;
    }
  }

  deque_type<bool>                m_record_status;
  multiseries_main_container_type m_series;
  string_store_pointer_type       m_string_store;
};

using record_store = basic_record_store<>;
}  // namespace multiseries
