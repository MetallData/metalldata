// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <iostream>
#include <json_bento/box/accessor_fwd.hpp>
#include <string>

namespace json_bento::jbdtl {
template <typename allocator_type>
inline void pretty_print_helper(std::ostream&                         os,
                                const array_accessor<allocator_type>& ja,
                                const std::string&                    indent,
                                const int indent_size) {
  std::string new_indent = indent;
  new_indent.append(indent_size, ' ');
  os << "[\n";
  for (std::size_t i = 0; i < ja.size(); ++i) {
    os << new_indent;
    pretty_print_helper(os, ja[i], new_indent, indent_size);
    if (i < ja.size() - 1) {
      os << ",\n";
    }
  }
  os << "\n" << indent << "]";
}

template <typename allocator_type>
inline void pretty_print_helper(std::ostream&                          os,
                                const object_accessor<allocator_type>& jo,
                                const std::string&                     indent,
                                const int indent_size) {
  os << "{\n";
  std::string new_indent = indent;
  new_indent.append(indent_size, ' ');
  for (auto it = jo.begin();;) {
    const auto kv = *it;
    os << new_indent << kv.key() << " : ";
    pretty_print_helper(os, kv.value(), new_indent, indent_size);
    if (++it == jo.end()) {
      break;
    }
    os << ",\n";
  }
  os << "\n" << indent << "}";
}

template <typename allocator_type>
inline void pretty_print_helper(std::ostream&                         os,
                                const value_accessor<allocator_type>& jv,
                                const std::string&                    indent,
                                const int indent_size) {
  if (jv.is_bool()) {
    os << std::boolalpha << jv.as_bool();
  } else if (jv.is_int64()) {
    os << jv.as_int64();
  } else if (jv.is_uint64()) {
    os << jv.as_uint64();
  } else if (jv.is_double()) {
    os << jv.as_double();
  } else if (jv.is_string()) {
    os << jv.as_string();
  } else if (jv.is_array()) {
    const auto& arr = jv.as_array();
    pretty_print_helper(os, arr, indent, indent_size);
  } else if (jv.is_object()) {
    const auto& obj = jv.as_object();
    pretty_print_helper(os, obj, indent, indent_size);
  } else if (jv.is_null()) {
    os << "null";
  }
}
}  // namespace json_bento::jbdtl

namespace json_bento {
template <typename allocator_type>
inline void pretty_print(const jbdtl::value_accessor<allocator_type>& jv,
                         std::ostream& os            = std::cout,
                         const int     indent_size   = 2,
                         const bool    print_newline = true) {
  std::string indent;
  jbdtl::pretty_print_helper(os, jv, indent, indent_size);
  if (print_newline) os << std::endl;
}

template <typename allocator_type>
inline void pretty_print(const jbdtl::object_accessor<allocator_type>& jo,
                         std::ostream& os            = std::cout,
                         const int     indent_size   = 2,
                         const bool    print_newline = true) {
  std::string indent;
  jbdtl::pretty_print_helper(os, jo, indent, indent_size);
  if (print_newline) os << std::endl;
}

template <typename allocator_type>
inline void pretty_print(const jbdtl::array_accessor<allocator_type>& ja,
                         std::ostream& os            = std::cout,
                         const int     indent_size   = 2,
                         const bool    print_newline = true) {
  std::string indent;
  jbdtl::pretty_print_helper(os, ja, indent, indent_size);
  if (print_newline) os << std::endl;
}
}  // namespace json_bento

template <typename allocator_type>
std::ostream& operator<<(
    std::ostream&                                            os,
    const json_bento::jbdtl::value_accessor<allocator_type>& jv) {
  json_bento::pretty_print(jv, os, 2, false);
  return os;
}

template <typename allocator_type>
std::ostream& operator<<(
    std::ostream&                                            os,
    const json_bento::jbdtl::array_accessor<allocator_type>& ja) {
  json_bento::pretty_print(ja, os, 2, false);
  return os;
}

template <typename allocator_type>
std::ostream& operator<<(
    std::ostream&                                             os,
    const json_bento::jbdtl::object_accessor<allocator_type>& jo) {
  json_bento::pretty_print(jo, os, 2, false);
  return os;
}