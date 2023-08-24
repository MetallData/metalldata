// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <json_bento/box/core_data/value_locator.hpp>

using namespace json_bento::jbdtl;

void checK_null(const value_locator loc) {
  EXPECT_TRUE(loc.is_null());
  EXPECT_FALSE(loc.is_bool());
  EXPECT_FALSE(loc.is_int64());
  EXPECT_FALSE(loc.is_uint64());
  EXPECT_FALSE(loc.is_double());
  EXPECT_FALSE(loc.is_string_index());
  EXPECT_FALSE(loc.is_array_index());
  EXPECT_FALSE(loc.is_object_index());
  EXPECT_FALSE(loc.is_primitive());
  EXPECT_FALSE(loc.is_index());
}

TEST(ValueLocatorTest, Everything) {
  {
    value_locator loc;
    checK_null(loc);
  }

  {
    value_locator loc;
    loc.emplace_bool() = true;

    EXPECT_FALSE(loc.is_null());
    EXPECT_TRUE(loc.is_bool());
    EXPECT_FALSE(loc.is_int64());
    EXPECT_FALSE(loc.is_uint64());
    EXPECT_FALSE(loc.is_double());
    EXPECT_FALSE(loc.is_string_index());
    EXPECT_FALSE(loc.is_array_index());
    EXPECT_FALSE(loc.is_object_index());
    EXPECT_TRUE(loc.is_primitive());
    EXPECT_FALSE(loc.is_index());

    EXPECT_EQ(loc.as_bool(), true);

    loc.reset();
    checK_null(loc);
  }

  {
    value_locator loc;
    loc.emplace_int64() = -1;

    EXPECT_FALSE(loc.is_null());
    EXPECT_FALSE(loc.is_bool());
    EXPECT_TRUE(loc.is_int64());
    EXPECT_FALSE(loc.is_uint64());
    EXPECT_FALSE(loc.is_double());
    EXPECT_FALSE(loc.is_string_index());
    EXPECT_FALSE(loc.is_array_index());
    EXPECT_FALSE(loc.is_object_index());
    EXPECT_TRUE(loc.is_primitive());
    EXPECT_FALSE(loc.is_index());

    EXPECT_EQ(loc.as_int64(), -1);

    loc.reset();
    checK_null(loc);
  }

  {
    value_locator loc;
    loc.emplace_uint64() = 1;

    EXPECT_FALSE(loc.is_null());
    EXPECT_FALSE(loc.is_bool());
    EXPECT_FALSE(loc.is_int64());
    EXPECT_TRUE(loc.is_uint64());
    EXPECT_FALSE(loc.is_double());
    EXPECT_FALSE(loc.is_string_index());
    EXPECT_FALSE(loc.is_array_index());
    EXPECT_FALSE(loc.is_object_index());
    EXPECT_TRUE(loc.is_primitive());
    EXPECT_FALSE(loc.is_index());

    EXPECT_EQ(loc.as_uint64(), 1);

    loc.reset();
    checK_null(loc);
  }

  {
    value_locator loc;
    loc.emplace_double() = 1.5;

    EXPECT_FALSE(loc.is_null());
    EXPECT_FALSE(loc.is_bool());
    EXPECT_FALSE(loc.is_int64());
    EXPECT_FALSE(loc.is_uint64());
    EXPECT_TRUE(loc.is_double());
    EXPECT_FALSE(loc.is_string_index());
    EXPECT_FALSE(loc.is_array_index());
    EXPECT_FALSE(loc.is_object_index());
    EXPECT_TRUE(loc.is_primitive());
    EXPECT_FALSE(loc.is_index());

    EXPECT_EQ(loc.as_double(), 1.5);

    loc.reset();
    checK_null(loc);
  }

  {
    value_locator loc;
    loc.emplace_string_index() = 10;

    EXPECT_FALSE(loc.is_null());
    EXPECT_FALSE(loc.is_bool());
    EXPECT_FALSE(loc.is_int64());
    EXPECT_FALSE(loc.is_uint64());
    EXPECT_FALSE(loc.is_double());
    EXPECT_TRUE(loc.is_string_index());
    EXPECT_FALSE(loc.is_array_index());
    EXPECT_FALSE(loc.is_object_index());
    EXPECT_FALSE(loc.is_primitive());
    EXPECT_TRUE(loc.is_index());

    EXPECT_EQ(loc.as_index(), 10);

    loc.reset();
    checK_null(loc);
  }

  {
    value_locator loc;
    loc.emplace_array_index() = 20;

    EXPECT_FALSE(loc.is_null());
    EXPECT_FALSE(loc.is_bool());
    EXPECT_FALSE(loc.is_int64());
    EXPECT_FALSE(loc.is_uint64());
    EXPECT_FALSE(loc.is_double());
    EXPECT_FALSE(loc.is_string_index());
    EXPECT_TRUE(loc.is_array_index());
    EXPECT_FALSE(loc.is_object_index());
    EXPECT_FALSE(loc.is_primitive());
    EXPECT_TRUE(loc.is_index());

    EXPECT_EQ(loc.as_index(), 20);

    loc.reset();
    checK_null(loc);
  }

  {
    value_locator loc;
    loc.emplace_object_index() = 30;

    EXPECT_FALSE(loc.is_null());
    EXPECT_FALSE(loc.is_bool());
    EXPECT_FALSE(loc.is_int64());
    EXPECT_FALSE(loc.is_uint64());
    EXPECT_FALSE(loc.is_double());
    EXPECT_FALSE(loc.is_string_index());
    EXPECT_FALSE(loc.is_array_index());
    EXPECT_TRUE(loc.is_object_index());
    EXPECT_FALSE(loc.is_primitive());
    EXPECT_TRUE(loc.is_index());

    EXPECT_EQ(loc.as_index(), 30);

    loc.reset();
    checK_null(loc);
  }
}