// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <iterator>
#include <memory>
#include <string>

#include <metall/json/json.hpp>

#include <json_bento/boost_json.hpp>
#include <json_bento/box/array_accessor.hpp>
#include <json_bento/box/core_data/core_data.hpp>
#include <json_bento/box/key_value_pair_accessor.hpp>
#include <json_bento/box/object_accessor.hpp>
#include <json_bento/box/string_accessor.hpp>
#include <json_bento/box/value_accessor.hpp>
#include <json_bento/pretty_print.hpp>
#include <json_bento/value_to.hpp>

namespace json_bento {

namespace {
namespace bj = boost::json;
namespace mj = metall::json;
}  // namespace

/// \brief Memory-efficient JSON store
/// that adds items sequentially and provides array-like indexing,
/// i.e., index range is [0, N - 1], where N is the number of items at the time.
/// \tparam Alloc Allocator type.
template <typename Alloc = std::allocator<std::byte>>
class box {
 private:
  using self_type      = box<Alloc>;
  using core_data_type = jbdtl::core_data<Alloc>;
  using value_locator  = typename core_data_type::value_locator_type;

 public:
  using index_type      = std::size_t;
  using allocator_type  = Alloc;
  using value_accessor  = jbdtl::value_accessor<Alloc>;
  using object_accessor = jbdtl::object_accessor<Alloc>;
  using array_accessor  = jbdtl::array_accessor<Alloc>;

  box() = default;

  explicit box(const allocator_type alloc) : m_box(alloc) {}

  /// \brief Access the item that is associated with a specified ID.
  /// \param index ID of the item to access.
  /// \return Returns a value accessor instance.
  value_accessor operator[](const index_type index) {
    return value_accessor(value_accessor::value_type_tag::root, index, &m_box);
  }

  /// \brief Access the item that is associated with a specified ID.
  /// \param index ID of the item to access.
  /// \return Returns a value accessor instance.
  value_accessor operator[](const index_type index) const {
    return value_accessor(value_accessor::value_type_tag::root, index,
                          &const_cast<self_type*>(this)->m_box);
  }

  /// \brief Access the item that is associated with a specified ID.
  /// \param index ID of the item to access.
  /// \return Returns a value accessor instance.
  value_accessor at(const index_type index) { return (*this)[index]; }

  /// \brief Access the item that is associated with a specified ID.
  /// \param index ID of the item to access.
  /// \return Returns a value accessor instance.
  value_accessor at(const index_type index) const { return (*this)[index]; }

  /// \brief Access the last item.
  /// \return Returns a value accessor instance.
  value_accessor back() { return at(size() - 1); }

  /// \brief Add an empty item at the end.
  /// \return Returns the ID of the added item.
  index_type push_back() {
    return push_back_root_value(boost::json::value{}, m_box);
  }

  /// \brief Add an item at the end.
  /// \param value Value to add.
  /// \return Returns the ID of the added item.
  index_type push_back(mj::value<allocator_type> value) {
    return push_back_root_value(value, m_box);
  }

  /// \brief Add an item at the end.
  /// \param value Value to add.
  /// value can be any data type that can be parsed by boost::json::value.
  /// \return Returns the ID of the added item.
  /// \example
  /// \code
  /// box jb;
  /// jb.push_back({{"key1", "value1" }, { "key2", 42 }});
  /// \endcode
  index_type push_back(boost::json::value value) {
    return push_back_root_value(value, m_box);
  }

  /// \brief Add an item at the end.
  /// \param value Value to add.
  /// \return Returns the ID of the added item.
  index_type push_back(value_accessor value) {
    // TODO: implement more efficient one
    return push_back(value_to<boost::json::value>(value));
  }

  /// \brief Return the number of items.
  /// \return Number of items.
  std::size_t size() const { return m_box.root_value_storage.size(); }

  /// \brief Erase all items.
  /// This function does not free all memory allocated for the items.
  void clear() {
    m_box.string_storage.clear();
    m_box.root_value_storage.clear();
    m_box.array_storage.clear();
    m_box.object_storage.clear();
    m_box.key_storage.clear();
  }

  /// \brief Shows statics of the JSON Bento instance.
  /// \param os Output stream to show the statistics.
  void profile(std::ostream& os = std::cout) const {
    os << "JSON Bento Profile" << std::endl;
    os << "#of root value data\t" << m_box.root_value_storage.size()
       << std::endl;
    os << "#of string data\t" << m_box.string_storage.size() << std::endl;
    os << "#of array data\t" << m_box.array_storage.size() << std::endl;
    os << "#of object data\t" << m_box.object_storage.size() << std::endl;
    os << "#of key data\t" << m_box.key_storage.size() << std::endl;
  }

 private:
  core_data_type m_box;
};
}  // namespace json_bento