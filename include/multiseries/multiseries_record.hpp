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

#include "string_table/string_store.hpp"

// Experimental feature
#ifndef METALLDATA_MSR_DEQUE_BLOCK_SIZE
#define METALLDATA_MSR_DEQUE_BLOCK_SIZE (1024UL * 1024 * 2)
#endif

namespace multiseries {

namespace {
namespace bc   = boost::container;
namespace cstr = compact_string;
}  // namespace

// enum class kind { int64, uint64, double_, string };
//
// template <typename T>
// constexpr kind get_kind() {
//   if constexpr (std::is_same_v<T, int64_t>) {
//     return kind::int64;
//   } else if constexpr (std::is_same_v<T, uint64_t>) {
//     return kind::uint64;
//   } else if constexpr (std::is_same_v<T, double>) {
//     return kind::double_;
//   } else if constexpr (std::is_same_v<T, std::string_view>) {
//     return kind::string;
//   } else {
//     static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>
//     ||
//                       std::is_same_v<T, double> || std::is_same_v<T,
//                       std::string_view>,
//                   "Unsupported type");
//   }
// }

/// \brief Column-based record store
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

  template <typename T>
  using vector_type = bc::vector<T, scp_allocator<T>>;

  using deque_block_option_t =
      bc::deque_options<bc::block_bytes<METALLDATA_MSR_DEQUE_BLOCK_SIZE>>::type;
  template <typename T>
  using deque_type = bc::deque<T, scp_allocator<T>, deque_block_option_t>;

  using series_store_type =
      std::variant<deque_type<int64_t>, deque_type<uint64_t>,
                   deque_type<double>, deque_type<cstr::string_accessor>>;
  template <typename T>
  struct get_series_store_type {
    // If T is std::string_view, type is cstr::string_accessor
    // Otherwise, type is T
    using type =
        deque_type<std::conditional_t<std::is_same_v<T, std::string_view>,
                                      cstr::string_accessor, T>>;
  };

  using string_type =
      bc::basic_string<char, std::char_traits<char>, other_allocator<char>>;

  struct series_item {
    string_type       name;
    deque_type<bool>  exist;
    series_store_type data;
  };
  using multiseries_store_type = vector_type<series_item>;

 public:
  using record_id_type            = size_t;
  using allocator_type            = Alloc;
  using string_store_type         = cstr::string_store<allocator_type>;
  using string_store_pointer_type = other_pointer_type<string_store_type>;
  template <typename T>
  struct series_info_type {
    using type = T;
    size_t series_index;
  };

  explicit basic_record_store(string_store_type    *string_store,
                              const allocator_type &alloc = allocator_type())
      : m_record_status(alloc), m_series(alloc), m_string_store(string_store) {}

  record_id_type add_record() {
    for (auto &item : m_series) {
      std::visit([](auto &series) { series.resize(series.size() + 1); },
                 item.data);
      item.exist.push_back(false);
    }
    m_record_status.push_back(true);
    return m_record_status.size() - 1;
  }

  template <typename series_type>
  series_info_type<series_type> add_series(std::string_view series_name) {
    priv_series_type_check<series_type>();

    // Check if the series already exists
    auto itr = priv_find_series(series_name);
    if (itr != m_series.end()) {
      return series_info_type<series_type>{
          .series_index =
              (size_t)std::abs(std::distance(m_series.begin(), itr))};
    }

    m_series.push_back(
        {.name  = string_type(series_name.data(), series_name.size(),
                              m_record_status.get_allocator()),
         .exist = deque_type<bool>(m_record_status.get_allocator()),
         .data  = typename get_series_store_type<series_type>::type(
             m_record_status.get_allocator())});

    return series_info_type<series_type>{.series_index = m_series.size() - 1};
  }

  template <typename series_type>
  const auto get(std::string_view     series_name,
                 const record_id_type record_id) const {
    priv_series_type_check<series_type>();
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found");
    }
    const auto &series = itr->data;
    return priv_access_series_data<series_type>(series, record_id);
  }

  template <typename series_type>
  const auto get(const series_info_type<series_type> &series_info,
                 const record_id_type                 record_id) const {
    priv_series_type_check<series_type>();
    if (series_info.series_index >= m_series.size()) {
      throw std::runtime_error("Series not found");
    }
    const auto &series = m_series[series_info.series_index].data;
    return priv_access_series_data<series_type>(series, record_id);
  }

  /// \brief Returns if the series data of record is None
  bool is_none(std::string_view     series_name,
               const record_id_type record_id) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      return true;
    }
    return !itr->exist[record_id];
  }

  template <typename series_type>
  bool is_none(const series_info_type<series_type> &series_info,
               const record_id_type                 record_id) const {
    if (series_info.series_index >= m_series.size()) {
      return true;
    }
    return !m_series[series_info.series_index].exist[record_id];
  }

  template <typename series_type>
  void set(std::string_view series_name, const record_id_type record_id,
           series_type value) {
    priv_series_type_check<series_type>();
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found");
    }

    priv_set_series_data<series_type>(*itr, record_id, value);
  }

  template <typename series_type>
  void set(const series_info_type<series_type> &series_info,
           const record_id_type record_id, series_type value) {
    priv_series_type_check<series_type>();
    if (series_info.series_index >= m_series.size()) {
      throw std::runtime_error("Series not found");
    }

    priv_set_series_data<series_type>(m_series[series_info.series_index],
                                      record_id, value);
  }

  /// Returns series_info_type<series_type>
  template <typename series_type>
  series_info_type<series_type> find_series(
      std::string_view series_name) const {
    priv_series_type_check<series_type>();
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      return series_info_type<series_type>{
          .series_index = std::numeric_limits<size_t>::max()};
    }
    return series_info_type<series_type>{
        .series_index = (size_t)std::abs(std::distance(m_series.begin(), itr))};
  }

  size_t num_records() const { return m_record_status.size(); }

  size_t num_series() const { return m_series.size(); }

  // series_func_t: [](int record_id, auto value) {}
  template <typename series_type, typename series_func_t>
  void for_all(std::string_view series_name, series_func_t series_func) const {
    auto itr = priv_find_series(series_name);
    if (itr == m_series.end()) {
      throw std::runtime_error("Series not found");
    }

    const auto &series_item = *itr;
    for (size_t i = 0; i < m_record_status.size(); ++i) {
      if (series_item.exist[i]) {
        series_func(i,
                    priv_access_series_data<series_type>(series_item.data, i));
      }
    }
  }

  template <typename series_type, typename series_func_t>
  void for_all(series_info_type<series_type> series_info,
               series_func_t                 series_func) const {
    if (series_info.series_index >= m_series.size()) {
      throw std::runtime_error("Series not found");
    }

    const auto &series_item = m_series[series_info.series_index];
    for (size_t i = 0; i < m_record_status.size(); ++i) {
      if (series_item.exist[i]) {
        series_func(i,
                    priv_access_series_data<series_type>(series_item.data, i));
      }
    }
  }

  bool contains(std::string_view series_name) const {
    return priv_find_series(series_name) != m_series.end();
  }

  std::vector<std::string> get_series_names() const {
    std::vector<std::string> series_names;
    for (const auto &item : m_series) {
      series_names.push_back(item.name);
    }
    return series_names;
  }

 private:
  template <class series_type>
  static constexpr void priv_series_type_check() {
    static_assert(std::is_same_v<series_type, int64_t> ||
                      std::is_same_v<series_type, uint64_t> ||
                      std::is_same_v<series_type, double> ||
                      std::is_same_v<series_type, std::string_view>,
                  "Unsupported series type");
  }

  multiseries_store_type::iterator priv_find_series(
      std::string_view series_name) {
    for (auto itr = m_series.begin(); itr != m_series.end(); ++itr) {
      if (itr->name == series_name) {
        return itr;
      }
    }
    return m_series.end();
  }

  multiseries_store_type::const_iterator priv_find_series(
      std::string_view series_name) const {
    for (auto itr = m_series.cbegin(); itr != m_series.cend(); ++itr) {
      if (itr->name == series_name) {
        return itr;
      }
    }
    return m_series.cend();
  }

  template <typename series_type>
  const auto priv_access_series_data(const series_store_type &series_store,
                                     const record_id_type     record_id) const {
    if constexpr (std::is_same_v<series_type, std::string_view>) {
      // Returns a string_view (not reference)
      return std::get<typename get_series_store_type<series_type>::type>(
                 series_store)[record_id]
          .to_view();
    } else {
      return std::get<typename get_series_store_type<series_type>::type>(
          series_store)[record_id];
    }
  }

  template <typename series_type>
  void priv_set_series_data(series_item &series, const record_id_type record_id,
                            const series_type &value) {
    series.exist[record_id] = true;
    if constexpr (std::is_same_v<series_type, std::string_view>) {
      auto accessor =
          cstr::add_string(value.data(), value.size(), *m_string_store);
      std::get<typename get_series_store_type<series_type>::type>(
          series.data)[record_id] = accessor;
    } else {
      std::get<typename get_series_store_type<series_type>::type>(
          series.data)[record_id] = value;
    }
  }

  deque_type<bool>          m_record_status;
  multiseries_store_type    m_series;
  string_store_pointer_type m_string_store;
};

using record_store = basic_record_store<>;

}  // namespace multiseries