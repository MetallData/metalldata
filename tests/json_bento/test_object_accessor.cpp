// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <json_bento/boost_json.hpp>
#include <json_bento/json_bento.hpp>

using box_type = json_bento::box<>;

TEST(ObjectAccessorTest, Init) {
  box_type           box;
  boost::json::value value;
  value.emplace_object();
  auto accessor = box[box.push_back(value)].as_object();
  EXPECT_EQ(accessor.size(), 0);
}

TEST(ObjectAccessorTest, Reference) {
  box_type           box;
  boost::json::value value;
  value.emplace_object();
  value.as_object()["init"] = true;
  auto accessor             = box[box.push_back(value)].as_object();
  EXPECT_EQ(accessor.size(), 1);
  EXPECT_TRUE(accessor.contains("init"));
  EXPECT_EQ(accessor.count("init"), 1);
  EXPECT_TRUE(accessor["init"].as_bool());

  accessor["key0"].emplace_bool() = true;
  EXPECT_TRUE(accessor["key0"].as_bool());
  accessor["key0"].as_bool() = false;
  EXPECT_FALSE(accessor["key0"].as_bool());
  EXPECT_TRUE(accessor.contains("key0"));
  EXPECT_EQ(accessor.count("key0"), 1);

  accessor["key1"].emplace_double() = 0.5;
  EXPECT_DOUBLE_EQ(accessor["key1"].as_double(), 0.5);
  EXPECT_FALSE(accessor["key0"].as_bool());
  EXPECT_TRUE(accessor.contains("key0"));
  EXPECT_TRUE(accessor.contains("key1"));
  EXPECT_EQ(accessor.count("key0"), 1);
  EXPECT_EQ(accessor.count("key1"), 1);

  // Const accessor should be able to see the same value
  const auto const_accessor = accessor;
  EXPECT_DOUBLE_EQ(const_accessor.at("key1").as_double(), 0.5);
  EXPECT_FALSE(const_accessor.at("key0").as_bool());
  EXPECT_TRUE(const_accessor.contains("init"));
  EXPECT_TRUE(const_accessor.contains("key0"));
  EXPECT_TRUE(const_accessor.contains("key1"));
  EXPECT_EQ(const_accessor.count("init"), 1);
  EXPECT_EQ(const_accessor.count("key0"), 1);
  EXPECT_EQ(const_accessor.count("key1"), 1);
}

TEST(ObjectAccessorTest, ConstReference) {
  box_type           box;
  boost::json::value value;
  value.emplace_object();
  value.as_object()["key0"] = true;
  value.as_object()["key1"] = 0.5;
  const auto const_accessor = box[box.push_back(value)].as_object();

  EXPECT_EQ(const_accessor.size(), 2);
  EXPECT_TRUE(const_accessor.at("key0").as_bool());
  EXPECT_DOUBLE_EQ(const_accessor.at("key1").as_double(), 0.5);
  EXPECT_TRUE(const_accessor.contains("key0"));
  EXPECT_TRUE(const_accessor.contains("key1"));
  EXPECT_EQ(const_accessor.count("key0"), 1);
  EXPECT_EQ(const_accessor.count("key1"), 1);
}

template <typename accessor_t>
void test_iterator_helper(accessor_t& accessor) {
  std::size_t cnt0 = 0;
  std::size_t cnt1 = 0;
  for (auto& item : accessor) {
    if (item.key() == "key0") {
      EXPECT_TRUE(item.value().as_bool());
      ++cnt0;
    } else if (item.key() == "key1") {
      EXPECT_EQ(item.value().as_double(), 0.5);
      ++cnt1;
    } else {
      ASSERT_TRUE(false);
    }
  }
  ASSERT_EQ(cnt0, 1);
  ASSERT_EQ(cnt1, 1);
}

TEST(ObjectAccessorTest, IteratorForEach) {
  box_type           box;
  boost::json::value value;
  value.emplace_object();
  value.as_object()["key0"] = true;
  value.as_object()["key1"] = 0.5;

  auto accessor = box[box.push_back(value)].as_object();
  {
    std::size_t cnt0 = 0;
    std::size_t cnt1 = 0;
    for (auto item : accessor) {
      if (item.key() == "key0") {
        EXPECT_TRUE(item.value().as_bool());
        ++cnt0;
      } else if (item.key() == "key1") {
        EXPECT_EQ(item.value().as_double(), 0.5);
        ++cnt1;
      } else {
        ASSERT_TRUE(false);
      }
    }
    ASSERT_EQ(cnt0, 1);
    ASSERT_EQ(cnt1, 1);
  }

  const auto& const_accessor = accessor;
  {
    std::size_t cnt0 = 0;
    std::size_t cnt1 = 0;
    for (const auto& item : const_accessor) {
      if (item.key() == "key0") {
        EXPECT_TRUE(item.value().as_bool());
        ++cnt0;
      } else if (item.key() == "key1") {
        EXPECT_DOUBLE_EQ(item.value().as_double(), 0.5);
        ++cnt1;
      } else {
        ASSERT_TRUE(false);
      }
    }
    ASSERT_EQ(cnt0, 1);
    ASSERT_EQ(cnt1, 1);
  }

  // Can change values
  {
    for (auto item : accessor) {
      if (item.key() == "key0") {
        item.value().emplace_int64() = 10;
      } else if (item.key() == "key1") {
        item.value().emplace_string() = "val1";
      }
    }
  }

  {
    std::size_t cnt0 = 0;
    std::size_t cnt1 = 0;
    for (const auto& item : const_accessor) {
      if (item.key() == "key0") {
        EXPECT_EQ(item.value().as_int64(), 10);
        ++cnt0;
      } else if (item.key() == "key1") {
        EXPECT_STREQ(item.value().as_string().c_str(), "val1");
        ++cnt1;
      } else {
        ASSERT_TRUE(false);
      }
    }
    ASSERT_EQ(cnt0, 1);
    ASSERT_EQ(cnt1, 1);
  }
}

template <typename T>
int check_iterator_value(const T it) {
  if (it->key() == "key0") {
    EXPECT_TRUE((*it).value().as_bool());
    EXPECT_TRUE(it->value().as_bool());
    return 1;
  } else if (it->key() == "key1") {
    EXPECT_EQ((*it).value().as_double(), 0.5);
    EXPECT_EQ(it->value().as_double(), 0.5);
    return 10;
  }
  return -1;
}

TEST(ObjectAccessorTest, Iterator) {
  box_type           box;
  boost::json::value value;
  value.emplace_object();
  value.as_object()["key0"] = true;
  value.as_object()["key1"] = 0.5;

  auto accessor = box[box.push_back(value)].as_object();

  {
    auto       it = accessor.begin();
    const auto x0 = check_iterator_value(it);

    ++it;
    const auto x1 = check_iterator_value(it);

    EXPECT_TRUE((x0 == 1 && x1 == 10) || (x0 == 10 && x1 == 1))
        << "++it did not move to the next element";

    auto old_it = it++;
    EXPECT_EQ(check_iterator_value(old_it), x1);
    EXPECT_EQ(it, accessor.end());
  }
}

TEST(ObjectAccessorTest, Find) {
  box_type           box;
  boost::json::value value;
  value.emplace_object();
  value.as_object()["key0"] = true;
  value.as_object()["key1"] = 0.5;

  auto accessor = box[box.push_back(value)].as_object();
  EXPECT_NE(accessor.find("key0"), accessor.end());
  EXPECT_NE(accessor.find("key1"), accessor.end());
  EXPECT_EQ(accessor.find("key2"), accessor.end());
  EXPECT_TRUE((*accessor.find("key0")).value().as_bool());
  EXPECT_DOUBLE_EQ((*accessor.find("key1")).value().as_double(), 0.5);

  const auto& const_accessor = accessor;
  EXPECT_NE(const_accessor.find("key0"), const_accessor.end());
  EXPECT_NE(const_accessor.find("key1"), const_accessor.end());
  EXPECT_EQ(const_accessor.find("key2"), const_accessor.end());
  EXPECT_TRUE((*const_accessor.find("key0")).value().as_bool());
  EXPECT_DOUBLE_EQ((*const_accessor.find("key1")).value().as_double(), 0.5);
}

TEST(ObjectAccessorTest, IfContains) {
  box_type           box;
  boost::json::value value;
  value.emplace_object();
  value.as_object()["key0"] = true;
  value.as_object()["key1"] = 0.5;

  auto accessor = box[box.push_back(value)].as_object();
  EXPECT_EQ(accessor.if_contains("key0")->as_bool(), true);
  EXPECT_DOUBLE_EQ(accessor.if_contains("key1")->as_double(), 0.5);
  EXPECT_FALSE(accessor.if_contains("key2"));

  const auto& const_accessor = accessor;
  EXPECT_EQ(const_accessor.if_contains("key0")->as_bool(), true);
  EXPECT_DOUBLE_EQ(const_accessor.if_contains("key1")->as_double(), 0.5);
  EXPECT_FALSE(const_accessor.if_contains("key2"));
}