// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <boost/json/src.hpp>
#include <json_bento/json_bento.hpp>

using box_type = json_bento::box<>;

TEST(ValueAccessorTest, IsType) {
  box_type box;

  {
    boost::json::value bjv;
    bjv.emplace_null();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_TRUE(accessor.is_null());
    EXPECT_FALSE(accessor.is_bool());
    EXPECT_FALSE(accessor.is_int64());
    EXPECT_FALSE(accessor.is_uint64());
    EXPECT_FALSE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_FALSE(accessor.is_array());
    EXPECT_FALSE(accessor.is_object());
  }

  {
    boost::json::value bjv;
    bjv.emplace_bool();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_FALSE(accessor.is_null());
    EXPECT_TRUE(accessor.is_bool());
    EXPECT_FALSE(accessor.is_int64());
    EXPECT_FALSE(accessor.is_uint64());
    EXPECT_FALSE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_FALSE(accessor.is_array());
    EXPECT_FALSE(accessor.is_object());
  }

  {
    boost::json::value bjv;
    bjv.emplace_int64();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_FALSE(accessor.is_null());
    EXPECT_FALSE(accessor.is_bool());
    EXPECT_TRUE(accessor.is_int64());
    EXPECT_FALSE(accessor.is_uint64());
    EXPECT_FALSE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_FALSE(accessor.is_array());
    EXPECT_FALSE(accessor.is_object());
  }

  {
    boost::json::value bjv;
    bjv.emplace_uint64();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_FALSE(accessor.is_null());
    EXPECT_FALSE(accessor.is_bool());
    EXPECT_FALSE(accessor.is_int64());
    EXPECT_TRUE(accessor.is_uint64());
    EXPECT_FALSE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_FALSE(accessor.is_array());
    EXPECT_FALSE(accessor.is_object());
  }

  {
    boost::json::value bjv;
    bjv.emplace_uint64();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_FALSE(accessor.is_null());
    EXPECT_FALSE(accessor.is_bool());
    EXPECT_FALSE(accessor.is_int64());
    EXPECT_TRUE(accessor.is_uint64());
    EXPECT_FALSE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_FALSE(accessor.is_array());
    EXPECT_FALSE(accessor.is_object());
  }

  {
    boost::json::value bjv;
    bjv.emplace_double();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_FALSE(accessor.is_null());
    EXPECT_FALSE(accessor.is_bool());
    EXPECT_FALSE(accessor.is_int64());
    EXPECT_FALSE(accessor.is_uint64());
    EXPECT_TRUE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_FALSE(accessor.is_array());
    EXPECT_FALSE(accessor.is_object());
  }

  {
    boost::json::value bjv;
    bjv.emplace_array();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_FALSE(accessor.is_null());
    EXPECT_FALSE(accessor.is_bool());
    EXPECT_FALSE(accessor.is_int64());
    EXPECT_FALSE(accessor.is_uint64());
    EXPECT_FALSE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_TRUE(accessor.is_array());
    EXPECT_FALSE(accessor.is_object());
  }

  {
    boost::json::value bjv;
    bjv.emplace_object();
    const auto id       = box.push_back(bjv);
    const auto accessor = box[id];
    EXPECT_FALSE(accessor.is_null());
    EXPECT_FALSE(accessor.is_bool());
    EXPECT_FALSE(accessor.is_int64());
    EXPECT_FALSE(accessor.is_uint64());
    EXPECT_FALSE(accessor.is_double());
    EXPECT_FALSE(accessor.is_string());
    EXPECT_FALSE(accessor.is_array());
    EXPECT_TRUE(accessor.is_object());
  }
}

TEST(ValueAccessorTest, AsType) {
  box_type box;

  {
    boost::json::value bjv            = static_cast<bool>(true);
    const auto         id             = box.push_back(bjv);
    const auto         const_accessor = box[id];
    EXPECT_EQ(const_accessor.as_bool(), true);

    auto accessor = box[id];
    EXPECT_EQ(accessor.as_bool(), true);

    accessor.as_bool() = false;
    EXPECT_EQ(const_accessor.as_bool(), false);
    EXPECT_EQ(accessor.as_bool(), false);
  }

  {
    boost::json::value bjv            = static_cast<int64_t>(10);
    const auto         id             = box.push_back(bjv);
    const auto         const_accessor = box[id];
    EXPECT_EQ(const_accessor.as_int64(), 10);

    auto accessor = box[id];
    EXPECT_EQ(accessor.as_int64(), 10);

    accessor.as_int64() = -20;
    EXPECT_EQ(const_accessor.as_int64(), -20);
    EXPECT_EQ(accessor.as_int64(), -20);
  }

  {
    boost::json::value bjv            = static_cast<uint64_t>(10);
    const auto         id             = box.push_back(bjv);
    const auto         const_accessor = box[id];
    EXPECT_EQ(const_accessor.as_uint64(), 10);

    auto accessor = box[id];
    EXPECT_EQ(accessor.as_uint64(), 10);

    accessor.as_uint64() = 20;
    EXPECT_EQ(const_accessor.as_uint64(), 20);
    EXPECT_EQ(accessor.as_uint64(), 20);
  }

  {
    boost::json::value bjv            = static_cast<double>(0.5);
    const auto         id             = box.push_back(bjv);
    const auto         const_accessor = box[id];
    EXPECT_DOUBLE_EQ(const_accessor.as_double(), 0.5);

    auto accessor = box[id];
    EXPECT_DOUBLE_EQ(accessor.as_double(), 0.5);

    accessor.as_double() = -0.75;
    EXPECT_DOUBLE_EQ(const_accessor.as_double(), -0.75);
    EXPECT_DOUBLE_EQ(accessor.as_double(), -0.75);
  }

  {
    boost::json::value bjv;
    bjv.emplace_string()      = "test";
    const auto id             = box.push_back(bjv);
    const auto const_accessor = box[id];
    EXPECT_STREQ(const_accessor.as_string().c_str(), "test");

    auto accessor = box[id];
    EXPECT_STREQ(accessor.as_string().c_str(), "test");

    accessor.as_string() = "test-test";
    EXPECT_STREQ(const_accessor.as_string().c_str(), "test-test");
    EXPECT_STREQ(accessor.as_string().c_str(), "test-test");
  }

  {
    boost::json::value bjv;
    bjv.emplace_array();
    bjv.as_array().push_back(static_cast<int64_t>(10));
    const auto id             = box.push_back(bjv);
    const auto const_accessor = box[id];
    EXPECT_EQ(const_accessor.as_array()[0].as_int64(),
              static_cast<int64_t>(10));

    auto accessor = box[id];
    EXPECT_EQ(accessor.as_array()[0].as_int64(), static_cast<int64_t>(10));

    accessor.as_array()[0].emplace_double() = 0.1;
    EXPECT_DOUBLE_EQ(const_accessor.as_array()[0].as_double(), 0.1);
    EXPECT_DOUBLE_EQ(accessor.as_array()[0].as_double(), 0.1);
  }

  {
    boost::json::value bjv;
    bjv.emplace_object();
    bjv.as_object()["key"]    = static_cast<int64_t>(10);
    const auto id             = box.push_back(bjv);
    const auto const_accessor = box[id];
    EXPECT_EQ(const_accessor.as_object().at("key").as_int64(),
              static_cast<int64_t>(10));

    auto accessor = box[id];
    EXPECT_EQ(accessor.as_object().at("key").as_int64(),
              static_cast<int64_t>(10));

    accessor.as_object()["key"].emplace_double() = 0.1;
    EXPECT_DOUBLE_EQ(const_accessor.as_object().at("key").as_double(), 0.1);
    EXPECT_DOUBLE_EQ(accessor.as_object().at("key").as_double(), 0.1);
  }
}

TEST(ValueAccessorTest, Emplace) {
  box_type box;

  const auto id       = box.push_back(boost::json::value{});
  auto       accessor = box[id];

  accessor.emplace_null();
  EXPECT_TRUE(accessor.is_null());

  accessor.emplace_int64() = 10;
  EXPECT_TRUE(accessor.is_int64());
  EXPECT_EQ(accessor.as_int64(), 10);

  accessor.emplace_uint64() = -10;
  EXPECT_TRUE(accessor.is_uint64());
  EXPECT_EQ(accessor.as_uint64(), -10);

  accessor.emplace_double() = 0.1;
  EXPECT_TRUE(accessor.is_double());
  EXPECT_DOUBLE_EQ(accessor.as_double(), 0.1);

  accessor.emplace_string() = "test";
  EXPECT_TRUE(accessor.is_string());
  EXPECT_STREQ(accessor.as_string().c_str(), "test");

  accessor.emplace_array().emplace_back(static_cast<int64_t>(20));
  EXPECT_TRUE(accessor.is_array());
  EXPECT_EQ(accessor.as_array()[0].as_int64(), 20);

  accessor.emplace_object()["key"].emplace_int64() = 30;
  EXPECT_TRUE(accessor.is_object());
  EXPECT_EQ(accessor.as_object().at("key").as_int64(), 30);
}

TEST(ValueAccessorTest, Parse) {
  std::string json_string = R"(
      {
        "pi": 3.141,
        "happy": true,
        "name": "Alice",
        "nothing": null,
        "long key test long key test": {
          "everything": 42
        },
        "list": [1, 0, 2],
        "object": {
          "currency": "USD",
          "value": 42.99
        }
      }
    )";

  box_type box;

  const auto id       = box.push_back(boost::json::value{});
  auto       accessor = box[id];
  accessor.parse(json_string);
  EXPECT_EQ(accessor.as_object()["pi"].as_double(), 3.141);
  EXPECT_TRUE(accessor.as_object()["happy"].as_bool());
  EXPECT_STREQ(accessor.as_object()["name"].as_string().c_str(), "Alice");
  EXPECT_TRUE(accessor.as_object()["nothing"].is_null());
  EXPECT_EQ(accessor.as_object()["long key test long key test"]
                .as_object()["everything"]
                .as_int64(),
            42);
  EXPECT_EQ(accessor.as_object()["list"].as_array()[0].as_int64(), 1);
  EXPECT_EQ(accessor.as_object()["list"].as_array()[1].as_int64(), 0);
  EXPECT_EQ(accessor.as_object()["list"].as_array()[2].as_int64(), 2);
  EXPECT_STREQ(accessor.as_object()["object"]
                   .as_object()["currency"]
                   .as_string()
                   .c_str(),
               "USD");
  EXPECT_EQ(accessor.as_object()["object"].as_object()["value"].as_double(),
            42.99);
}

TEST(ValueAccessorTest, EqualOperator) {
  std::string json_string = R"(
      {
        "pi": 3.141,
        "happy": true,
        "name": "Alice",
        "nothing": null,
        "long key test long key test": {
          "everything": 42
        },
        "list": [1, 0, 2],
        "object": {
          "currency": "USD",
          "value": 42.99
        }
      }
    )";

  box_type box;

  const auto id0       = box.push_back(boost::json::parse(json_string));
  auto       accessor0 = box[id0];
  EXPECT_EQ(accessor0, accessor0);

  const auto id1       = box.push_back(boost::json::parse(json_string));
  auto       accessor1 = box[id1];
  EXPECT_EQ(accessor0, accessor1);

  accessor0.as_object()["pi"] = 3.14;
  EXPECT_NE(accessor0, accessor1);
}