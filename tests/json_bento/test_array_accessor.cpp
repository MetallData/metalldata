// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <boost/json/src.hpp>

#include <json_bento/json_bento.hpp>

using bento_type = json_bento::box<>;

TEST(ArrayAccessorTest, Init) {
  bento_type         bento;
  boost::json::value value;
  value.emplace_array();
  auto accessor = bento[bento.push_back(value)].as_array();
  EXPECT_EQ(accessor.size(), 0);
}

TEST(ArrayAccessorTest, Reference) {
  bento_type         bento;
  boost::json::value value;
  value.emplace_array();
  value.as_array().push_back(static_cast<int64_t>(10));
  value.as_array().push_back("val");

  auto accessor = bento[bento.push_back(value)].as_array();
  EXPECT_EQ(accessor.size(), 2);
  EXPECT_EQ(accessor[0].as_int64(), 10);
  EXPECT_STREQ(accessor[1].as_string().c_str(), "val");

  const auto const_accessor = accessor;
  EXPECT_EQ(const_accessor.size(), 2);
  EXPECT_EQ(const_accessor[0].as_int64(), 10);
  EXPECT_STREQ(const_accessor[1].as_string().c_str(), "val");
}

TEST(ArrayAccessorTest, Emplace) {
  bento_type         bento;
  boost::json::value value;
  value.emplace_array();
  auto accessor = bento[bento.push_back(value)].as_array();

  accessor.emplace_back(static_cast<int64_t>(10));
  EXPECT_EQ(accessor.size(), 1);
  EXPECT_EQ(accessor[0].as_int64(), 10);

  accessor.emplace_back("val");
  EXPECT_EQ(accessor.size(), 2);
  EXPECT_EQ(accessor[0].as_int64(), 10);
  EXPECT_STREQ(accessor[1].as_string().c_str(), "val");

  const auto const_accessor = accessor;
  EXPECT_EQ(const_accessor.size(), 2);
  EXPECT_EQ(const_accessor[0].as_int64(), 10);
  EXPECT_STREQ(const_accessor[1].as_string().c_str(), "val");
}

TEST(ArrayAccessorTest, Iterator) {
  bento_type         bento;
  boost::json::value value;
  value.emplace_array();
  value.as_array().push_back(static_cast<int64_t>(10));
  value.as_array().push_back("val");

  auto accessor = bento[bento.push_back(value)].as_array();

  {
    auto it = accessor.begin();
    EXPECT_EQ((*it).as_int64(), 10);
    EXPECT_EQ(it->as_int64(), 10);
    ++it;
    EXPECT_STREQ((*it).as_string().c_str(), "val");
    EXPECT_STREQ(it->as_string().c_str(), "val");

    auto old_it = it++;
    EXPECT_STREQ((*old_it).as_string().c_str(), "val");
    EXPECT_STREQ(old_it->as_string().c_str(), "val");
    EXPECT_EQ(it, accessor.end());

    --it;
    EXPECT_STREQ((*it).as_string().c_str(), "val");
    EXPECT_STREQ(it->as_string().c_str(), "val");

    old_it = it--;
    EXPECT_STREQ((*old_it).as_string().c_str(), "val");
    EXPECT_STREQ(old_it->as_string().c_str(), "val");
    EXPECT_EQ((*it).as_int64(), 10);
    EXPECT_EQ(it->as_int64(), 10);
    EXPECT_EQ(it, accessor.begin());
  }

  {
    const auto const_accessor = accessor;
    auto       cit            = const_accessor.begin();
    EXPECT_EQ((*cit).as_int64(), 10);
    EXPECT_EQ(cit->as_int64(), 10);
    ++cit;
    EXPECT_STREQ((*cit).as_string().c_str(), "val");
    EXPECT_STREQ(cit->as_string().c_str(), "val");
    auto old_cit = cit++;
    EXPECT_STREQ((*old_cit).as_string().c_str(), "val");
    EXPECT_STREQ(old_cit->as_string().c_str(), "val");
    EXPECT_EQ(cit, const_accessor.end());
  }

  // Can edit?
  {
    auto it = accessor.begin();

    (*it).as_int64() = 20;
    EXPECT_EQ(accessor[0].as_int64(), 20);

    it->as_int64() = 30;
    EXPECT_EQ(accessor[0].as_int64(), 30);

    ++it;
    (*it).as_string() = "val2";
    EXPECT_STREQ(accessor[1].as_string().c_str(), "val2");

    it->as_string() = "val3";
    EXPECT_STREQ(accessor[1].as_string().c_str(), "val3");
  }
}

TEST(ArrayAccessorTest, Resize) {
  bento_type         bento;
  boost::json::value value;
  value.emplace_array();
  value.as_array().resize(2);
  EXPECT_EQ(value.as_array().size(), 2);
}