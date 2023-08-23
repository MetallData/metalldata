// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <json_bento/box/core_data/key_locator.hpp>
#include <json_bento/box/core_data/value_locator.hpp>

namespace json_bento::jbdtl {

/// \brief Pair of key value locators
class key_value_pair {
 public:
  key_value_pair() = default;
  key_value_pair(key_locator key, value_locator value)
      : m_key(key), m_value(value) {}

  bool operator==(const key_value_pair& other) {
    return m_key == other.m_key && m_value == other.m_value;
  }

  bool operator!=(const key_value_pair& other) { return !(*this == other); }

  const key_locator& key() const { return m_key; }

  value_locator& value() { return m_value; };

  const value_locator& value() const { return m_value; };

 private:
  key_locator   m_key;
  value_locator m_value;
};

}  // namespace json_bento::jbdtl
