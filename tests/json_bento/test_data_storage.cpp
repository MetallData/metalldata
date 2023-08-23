// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <metall/metall.hpp>

#include <json_bento/details/data_storage.hpp>

using storage_type =
    json_bento::jbdtl::data_storage<int, metall::manager::allocator_type<int>>;

TEST(DataStorageTest, Everything) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    storage_type storage(manager.get_allocator());
    EXPECT_EQ(storage.size(), 0);

    const auto idx0 = storage.emplace(0);
    EXPECT_EQ(storage.at(idx0), 0);
    EXPECT_EQ(storage.size(), 1);

    const auto idx1 = storage.emplace(1);
    EXPECT_EQ(storage.at(idx1), 1);
    EXPECT_EQ(storage.size(), 2);

    storage.erase(idx1);
    EXPECT_EQ(storage.size(), 1);
    storage.erase(idx0);
    EXPECT_EQ(storage.size(), 0);
  }
}