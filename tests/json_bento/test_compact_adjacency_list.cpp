// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <json_bento/details/compact_adjacency_list.hpp>

using adj_type =
    json_bento::jbdtl::compact_adjacency_list<int, std::allocator<std::byte>>;

TEST(CompactAdjacencyListTest, AddRow) {
  adj_type list;
  EXPECT_EQ(list.size(), 0);

  EXPECT_EQ(list.push_back(), 0);
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 0);
}

TEST(CompactAdjacencyListTest, PushBack) {
  adj_type list;

  list.push_back(0, 1);
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 1);
  EXPECT_EQ(list.at(0, 0), 1);

  list.push_back(0, 2);
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 2);
  EXPECT_EQ(list.at(0, 0), 1);
  EXPECT_EQ(list.at(0, 1), 2);

  list.push_back(0, 3);
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 3);
  EXPECT_EQ(list.at(0, 0), 1);
  EXPECT_EQ(list.at(0, 1), 2);
  EXPECT_EQ(list.at(0, 2), 3);

  list.push_back(1, 4);
  EXPECT_EQ(list.size(), 2);
  EXPECT_EQ(list.size(0), 3);
  EXPECT_EQ(list.size(1), 1);
  EXPECT_EQ(list.at(0, 0), 1);
  EXPECT_EQ(list.at(0, 1), 2);
  EXPECT_EQ(list.at(0, 2), 3);
  EXPECT_EQ(list.at(1, 0), 4);
}

TEST(CompactAdjacencyListTest, Capacity) {
  adj_type list;
  EXPECT_EQ(list.capacity(), 0);

  list.push_back(0, 1);
  EXPECT_GE(list.capacity(), 1);
  EXPECT_GE(list.capacity(0), 1);

  list.push_back(0, 2);
  list.push_back(0, 3);
  EXPECT_GE(list.capacity(0), 3);

  list.push_back();
  EXPECT_GE(list.capacity(), 2);
}

TEST(CompactAdjacencyListTest, Size) {
  adj_type list;
  EXPECT_EQ(list.size(), 0);

  list.push_back(0, 1);
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 1);

  list.push_back(0, 2);
  list.push_back(0, 3);
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 3);

  list.push_back(1, 4);
  EXPECT_EQ(list.size(), 2);
  EXPECT_EQ(list.size(1), 1);
}

TEST(CompactAdjacencyListTest, Clear) {
  adj_type list;

  list.push_back(0, 1);
  list.push_back(0, 2);
  list.push_back(0, 3);
  list.push_back(1, 4);

  list.clear();
  EXPECT_EQ(list.size(), 0);
  EXPECT_GE(list.capacity(), 2);
}

TEST(CompactAdjacencyListTest, ClearRow) {
  adj_type list;

  list.push_back(0, 1);
  list.push_back(0, 2);
  list.push_back(0, 3);
  list.push_back(1, 4);

  list.clear(0);
  EXPECT_EQ(list.size(0), 0);
  EXPECT_GE(list.capacity(0), 3);
  EXPECT_EQ(list.size(1), 1);
  EXPECT_EQ(list.size(), 2);
}

TEST(CompactAdjacencyListTest, ShrinkToFit) {
  adj_type list;

  list.push_back(0, 1);
  list.push_back(0, 2);
  list.push_back(0, 3);
  list.push_back(1, 4);

  list.shrink_to_fit();
  EXPECT_EQ(list.size(0), 3);
  EXPECT_EQ(list.size(1), 1);
  EXPECT_EQ(list.size(), 2);
  EXPECT_GE(list.capacity(), 2);

  list.clear();
  list.shrink_to_fit();
  EXPECT_EQ(list.capacity(), 0);
}

TEST(CompactAdjacencyListTest, Resize) {
  adj_type list;
  list.resize(1);
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 0);
  list.push_back(0, 10);

  list.resize(4);  // Grow
  EXPECT_EQ(list.size(), 4);
  EXPECT_EQ(list.size(0), 1);
  EXPECT_EQ(list.size(1), 0);
  EXPECT_EQ(list.size(2), 0);
  EXPECT_EQ(list.size(3), 0);
  EXPECT_EQ(list.at(0, 0), 10);

  list.resize(1);  // shrink
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.size(0), 1);
  EXPECT_EQ(list.at(0, 0), 10);
}