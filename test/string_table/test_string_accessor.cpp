// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#include <gtest/gtest.h>

#include <string_table/string_accessor.hpp>
#include <string_table/string_store.hpp>

using namespace compact_string;

TEST(StringAccessorTest, Type) { EXPECT_EQ(sizeof(string_accessor), 8); }

TEST(StringAccessorTest, Short) {
  for (uint i = 0; i <= string_accessor::short_str_max_length(); ++i) {
    std::string str(i, 'a');
    string_accessor accessor(str.c_str());
    EXPECT_TRUE(accessor.is_short());
    EXPECT_FALSE(accessor.is_long());
    EXPECT_EQ(accessor.length(), i);
    EXPECT_STREQ(accessor.c_str(), str.c_str());
    EXPECT_EQ(accessor.to_view().length(), str.length());
    EXPECT_STREQ(accessor.to_view().data(), str.c_str());

    // Test copy constructor
    string_accessor accessor2(accessor);
    EXPECT_TRUE(accessor2.is_short());
    EXPECT_EQ(accessor2.length(), i);
    EXPECT_STREQ(accessor2.c_str(), str.c_str());

    // Test move constructor
    string_accessor accessor3(std::move(accessor));
    EXPECT_TRUE(accessor3.is_short());
    EXPECT_EQ(accessor3.length(), i);
    EXPECT_STREQ(accessor3.c_str(), str.c_str());
  }
}

TEST(StringAccessorTest, Long) {
  for (uint i = string_accessor::short_str_max_length() + 1; i < 100; ++i) {
    std::string str(i, 'a');
    auto *str_with_length_ptr = csdtl::allocate_string_embedding_length<size_t>(
        std::string_view(str), std::allocator<char>());
    string_accessor accessor(&str_with_length_ptr[sizeof(size_t)], i);
    EXPECT_TRUE(accessor.is_long());
    EXPECT_FALSE(accessor.is_short());
    EXPECT_EQ(accessor.length(), i);
    EXPECT_STREQ(accessor.c_str(), str.c_str());
    std::string_view view(str.c_str(), i);
    EXPECT_EQ(accessor.to_view(), view);
  }
}