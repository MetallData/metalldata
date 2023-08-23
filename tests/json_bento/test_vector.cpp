// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <json_bento/details/vector.hpp>

TEST(Vector, All) {
  json_bento::jbdtl::vector<int> v;

  EXPECT_EQ(v.size(), 0);
  EXPECT_GE(v.capacity(), 0);

  EXPECT_EQ(v.emplace_back(10), 10);
  EXPECT_EQ(v.size(), 1);
  EXPECT_GE(v.capacity(), 1);
  EXPECT_EQ(v[0], 10);
  EXPECT_EQ(v.at(0), 10);

  EXPECT_EQ(v.emplace_back(20), 20);
  EXPECT_EQ(v.size(), 2);
  EXPECT_GE(v.capacity(), 2);
  EXPECT_EQ(v[1], 20);
  EXPECT_EQ(v.at(1), 20);

  std::size_t found_10 = 0;
  std::size_t found_20 = 0;
  for (const auto item : v) {
    if (item == 10)
      found_10++;
    else if (item == 20)
      found_20++;
    else
      FAIL();
  }
  EXPECT_EQ(found_10, 1);
  EXPECT_EQ(found_20, 1);

  v.clear();
  EXPECT_EQ(v.size(), 0);
}