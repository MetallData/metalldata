// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include <json_bento/details/compact_string_storage.hpp>

using storage_t = json_bento::jbdtl::compact_string_storage<>;

template <typename storage_type>
void read_test_helper(
    const std::unordered_multimap<std::string, std::size_t>& ref_table,
    storage_type&                                            storage) {
  for (const auto& item : ref_table) {
    const auto* str = item.first.c_str();
    const auto  id  = item.second;
    EXPECT_STREQ(storage.at(id).c_str(), str);
    EXPECT_STREQ(storage[id].c_str(), str);
  }

  // Check iterator
  {
    auto copy_map = ref_table;
    for (const auto& item : storage) {
      EXPECT_GE(copy_map.count(item.c_str()), 1);
      copy_map.erase(copy_map.find(item.c_str()));  // erase only one item
    }
    EXPECT_TRUE(copy_map.empty());
  }
}

void test_helper(storage_t& storage) {
  // Make sure it's empy
  EXPECT_EQ(storage.size(), 0);
  for ([[maybe_unused]] const auto& item : storage) {
    FAIL();
  }

  std::unordered_multimap<std::string, std::size_t> ref_table;
  ref_table.emplace("test", 0);
  ref_table.emplace("long test string test test 0", 0);
  ref_table.emplace("test", 0);
  ref_table.emplace("long test string test test 1", 0);

  // Add strings, checking size()
  {
    std::size_t cnt = 0;
    for (auto& item : ref_table) {
      const auto& str = item.first;
      auto&       id  = item.second;
      id              = storage.emplace(str);
      ++cnt;
      EXPECT_EQ(storage.size(), cnt);
    }
  }

  read_test_helper(ref_table, storage);

  const auto& const_storage = storage;
  read_test_helper(ref_table, const_storage);

  // Erase
  {
    std::size_t cnt = ref_table.size();
    for (const auto& item : ref_table) {
      const auto& id = item.second;
      storage.erase(id);
      --cnt;
      EXPECT_EQ(storage.size(), cnt);
    }
  }
}

TEST(CompactStringStorage, All) {
  storage_t storage;

  {
    SCOPED_TRACE("No-const test");
    test_helper(storage);
  }

  {
    SCOPED_TRACE("Const test");
    test_helper(storage);
  }
}