// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <metall/metall.hpp>

#include <json_bento/details/key_store.hpp>

using store_type =
    json_bento::jbdtl::key_store<metall::manager::allocator_type<int>>;

TEST(CompactVectorTest, Everything) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    store_type store(manager.get_allocator());

    const auto loc0 = store.find_or_add("key0");
    EXPECT_EQ(store.find("key0"), loc0);
    EXPECT_EQ(store.find_or_add("key0"), loc0);  // No duplicate insert

    const auto loc1 = store.find_or_add("key1");
    EXPECT_EQ(store.find("key0"), loc0);
    EXPECT_EQ(store.find("key1"), loc1);

    EXPECT_STREQ(store.find(loc0).data(), "key0");
    EXPECT_STREQ(store.find(loc1).data(), "key1");
  }
}