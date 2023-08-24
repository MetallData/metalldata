// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <string>

#include <metall/metall.hpp>

#include <json_bento/box.hpp>

TEST(BoxTest, All) {
  metall::manager manager(metall::create_only, "/tmp/metall-test-dir");
  using bento_type =
      json_bento::box<metall::manager::allocator_type<std::byte>>;
  auto *bento = manager.construct<bento_type>(metall::unique_instance)(
      manager.get_allocator());

  std::string json_string = R"(
      {
        "number": 3.141,
        "bool": true,
        "string": "Alice Smith",
        "nothing": null,
        "object": {
          "everything": 42
        },
        "array": [1, 0, 2],
        "objects mixed types": {
          "currency": "USD",
          "values": [10.0, 20.1, 32.1]
        }
      }
    )";

  // Add an item.
  const auto index = bento->push_back(boost::json::parse(json_string));

  // Access the added item.
  auto value_accessor = bento->at(index);

  // Show the added item.
  EXPECT_EQ(json_bento::value_to<boost::json::value>(value_accessor),
            boost::json::parse(json_string));

  // -- Modify items --//
  auto object_accessor    = value_accessor.as_object();
  object_accessor["name"] = "Bob";

  auto array_accessor = object_accessor["answer"].emplace_array();
  array_accessor.emplace_back(10);
  array_accessor.emplace_back(0.5);
  array_accessor.emplace_back("end");

  bento->clear();
  EXPECT_EQ(bento->size(), 0);
}