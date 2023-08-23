// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <boost/json/src.hpp>

#include <json_bento/json_bento.hpp>

using box_type = json_bento::box<>;

TEST(StringAccessorTest, Empty) {
  {
    boost::json::value bj_string;
    bj_string.emplace_string();

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_TRUE(sa.empty());
  }

  {
    boost::json::value bj_string;
    bj_string.emplace_string() = "Hello, world!";

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_FALSE(sa.empty());
  }
}

TEST(StringAccessorTest, Size) {
  {
    boost::json::value bj_string;
    bj_string.emplace_string();

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_EQ(sa.size(), 0);
    EXPECT_EQ(sa.length(), 0);
  }

  {
    boost::json::value bj_string;
    bj_string.emplace_string() = "Hello";

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_EQ(sa.size(), 5);
    EXPECT_EQ(sa.length(), 5);
  }
}

TEST(StringAccessorTest, CStr) {
  {
    boost::json::value bj_string;
    bj_string.emplace_string();

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_STREQ(sa.c_str(), "");
  }

  {
    boost::json::value bj_string;
    bj_string.emplace_string() = "Hello, world!";

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_STREQ(sa.c_str(), "Hello, world!");
  }
}

TEST(StringAccessorTest, Data) {
  {
    boost::json::value bj_string;
    bj_string.emplace_string();

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_STREQ(sa.data(), "");
  }

  {
    boost::json::value bj_string;
    bj_string.emplace_string() = "Hello, world!";

    box_type   box;
    const auto id = box.push_back(bj_string);
    auto       sa = box[id].as_string();
    EXPECT_STREQ(sa.data(), "Hello, world!");
  }
}

TEST(StringAccessorTest, Clear) {
  boost::json::value bj_string;
  bj_string.emplace_string() = "Hello, world!";

  box_type   box;
  const auto id = box.push_back(bj_string);
  auto       sa = box[id].as_string();
  EXPECT_STREQ(sa.c_str(), "Hello, world!");
  sa.clear();
  EXPECT_STREQ(sa.c_str(), "");
  EXPECT_TRUE(sa.empty());
  EXPECT_EQ(sa.size(), 0);

  // Check that the underlying value is also cleared
  EXPECT_STREQ(box[id].as_string().c_str(), "");
}

TEST(StringAccessorTest, Assign) {
  boost::json::value bj_string;
  bj_string.emplace_string() = "Hello, world!";

  box_type   box;
  const auto id = box.push_back(bj_string);
  auto       sa = box[id].as_string();
  EXPECT_STREQ(sa.c_str(), "Hello, world!");
  sa = "Goodbye, world!";
  EXPECT_STREQ(sa.c_str(), "Goodbye, world!");

  // Check that the underlying value is also assigned
  EXPECT_STREQ(box[id].as_string().c_str(), "Goodbye, world!");
}

TEST(StringAccessorTest, Iterator) {
  boost::json::value bj_string;
  bj_string.emplace_string() = "Hello, world!";

  box_type   box;
  const auto id = box.push_back(bj_string);
  auto       sa = box[id].as_string();

  auto it = sa.begin();
  EXPECT_EQ(*it, 'H');
  ++it;
  EXPECT_EQ(*it, 'e');
  ++it;
  EXPECT_EQ(*it, 'l');
  ++it;
  EXPECT_EQ(*it, 'l');
  ++it;
  EXPECT_EQ(*it, 'o');
  ++it;
  EXPECT_EQ(*it, ',');
  ++it;
  EXPECT_EQ(*it, ' ');
  ++it;
  EXPECT_EQ(*it, 'w');
  ++it;
  EXPECT_EQ(*it, 'o');
  ++it;
  EXPECT_EQ(*it, 'r');
  ++it;
  EXPECT_EQ(*it, 'l');
  ++it;
  EXPECT_EQ(*it, 'd');
  ++it;
  EXPECT_EQ(*it, '!');
  ++it;
  EXPECT_EQ(it, sa.end());
}

TEST(StringAccessorTest, Conversion) {
  boost::json::value bj_string;
  bj_string.emplace_string() = "Hello, world!";

  box_type   box;
  const auto id = box.push_back(bj_string);
  auto       sa = box[id].as_string();

  std::string s = static_cast<std::string>(sa);
  EXPECT_EQ(s, "Hello, world!");

  std::string_view sv = static_cast<std::string_view>(sa);
  EXPECT_EQ(sv, "Hello, world!");
}