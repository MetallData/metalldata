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

TEST(ValueToTest, BoostJSON) {
  box_type box;
  box.push_back(boost::json::parse(json_string));
  auto val = json_bento::value_to<boost::json::value>(box.back());
  EXPECT_EQ(val, boost::json::parse(json_string));
}

TEST(ValueToTest, MetallJSON) {
  box_type box;
  box.push_back(boost::json::parse(json_string));
  metall::json::value mj_value;
  json_bento::value_to(box.back(), mj_value);
  EXPECT_EQ(mj_value, metall::json::parse(json_string));
}