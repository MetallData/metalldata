// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#include <gtest/gtest.h>

#include <metall/metall.hpp>

#include <string_table/string_store.hpp>

using store_type =
  compact_string::string_store<metall::manager::allocator_type<std::byte>>;
using char_pointer = metall::offset_ptr<const char>;

// Demonstrate some basic assertions.
TEST(StringTableTest, Basic) {
  {
    metall::manager manager(metall::create_only, "/tmp/metall-test");
    auto *store = manager.construct<store_type>(metall::unique_instance)(
      manager.get_allocator());

    auto *ptr0 = manager.construct<char_pointer>("ptr0")();
    *ptr0      = store->find_or_add("key0");

    EXPECT_EQ(store->find("key0"), *ptr0);
    EXPECT_EQ(store->find_or_add("key0"), *ptr0);  // No duplicate insert

    auto *ptr1 = manager.construct<char_pointer>("ptr1")();
    *ptr1      = store->find_or_add("key1");
    EXPECT_STREQ(store->find("key0"), ptr0->get());
    EXPECT_STREQ(store->find("key1"), ptr1->get());

    EXPECT_EQ(store->size(), size_t(2));
  }

  // Open and check values
  {
    metall::manager manager(metall::open_read_only, "/tmp/metall-test");
    auto *store = manager.find<store_type>(metall::unique_instance).first;

    EXPECT_STREQ(store->find("key0"), "key0");
    EXPECT_STREQ(store->find("key1"), "key1");

    auto *ptr0 = manager.find<char_pointer>("ptr0").first;
    auto *ptr1 = manager.find<char_pointer>("ptr1").first;
    EXPECT_EQ(store->find("key0"), *ptr0);
    EXPECT_EQ(store->find("key1"), *ptr1);

    EXPECT_EQ(store->size(), size_t(2));
  }
}

TEST(StringTableTest, AddString) {
  // test add_string()
  {
    metall::manager manager(metall::create_only, "/tmp/metall-test");
    auto *store = manager.construct<store_type>(metall::unique_instance)(
      manager.get_allocator());

    for (int len = 0; len < 100; ++len) {
      std::string str(len, 'a');
      auto        accessor = compact_string::add_string(str, *store);
      EXPECT_EQ(accessor.length(), len);
      EXPECT_STREQ(accessor.c_str(), str.c_str());
    }
  }
}