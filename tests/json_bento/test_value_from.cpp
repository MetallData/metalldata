// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <metall/metall.hpp>

#include <json_bento/boost_json.hpp>
#include <json_bento/json_bento.hpp>

using box_type = json_bento::box<>;

const std::string json_string = R"(
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

void check_value(const box_type::value_accessor accessor) {
  EXPECT_EQ(accessor.as_object().at("pi").as_double(), 3.141);
  EXPECT_TRUE(accessor.as_object().at("happy").as_bool());
  EXPECT_STREQ(accessor.as_object().at("name").as_string().c_str(), "Alice");
  EXPECT_TRUE(accessor.as_object().at("nothing").is_null());
  EXPECT_EQ(accessor.as_object()
                .at("long key test long key test")
                .as_object()
                .at("everything")
                .as_int64(),
            42);
  EXPECT_EQ(accessor.as_object().at("list").as_array()[0].as_int64(), 1);
  EXPECT_EQ(accessor.as_object().at("list").as_array()[1].as_int64(), 0);
  EXPECT_EQ(accessor.as_object().at("list").as_array()[2].as_int64(), 2);
  EXPECT_STREQ(accessor.as_object()
                   .at("object")
                   .as_object()
                   .at("currency")
                   .as_string()
                   .c_str(),
               "USD");
  EXPECT_EQ(
      accessor.as_object().at("object").as_object().at("value").as_double(),
      42.99);
}

TEST(ValueFromTest, BoostJSON) {
  boost::json::value value(boost::json::parse(json_string));

  box_type box;
  box.push_back();
  json_bento::value_from(value, box.back());
  check_value(box.back());
}

TEST(ValueFromTest, MetallJSON) {
  metall::manager manager(metall::create_only, "/tmp/json_bento_test");
  metall::json::value<metall::manager::allocator_type<std::byte>> value(
      metall::json::parse(json_string, manager.get_allocator()));

  box_type box;
  box.push_back();
  json_bento::value_from(value, box.back());
  check_value(box.back());
}