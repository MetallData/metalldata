// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <gtest/gtest.h>

#include <json_bento/details/compact_vector.hpp>
#include <metall/metall.hpp>

using vec_type =
    json_bento::jbdtl::compact_vector<int,
                                      metall::manager::allocator_type<int>>;

template <typename vec_t>
void test_helper(const std::size_t size, vec_t& vec) {
  EXPECT_EQ(vec.size(), size);

  for (std::size_t i = 0; i < size; ++i) {
    EXPECT_EQ(vec[i], i + 1);
    EXPECT_EQ(vec.at(i), i + 1);
  }

  std::size_t cnt = 0;
  for (auto elem : vec) {
    EXPECT_EQ(elem, cnt + 1);
    ++cnt;
  }
}

TEST(CompactVectorTest, Read) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    vec_type    vec;
    const auto& const_vec = vec;

    EXPECT_EQ(vec.size(), 0);
    EXPECT_EQ(const_vec.size(), 0);

    vec.push_back(1, manager.get_allocator());
    test_helper(1, vec);
    test_helper(1, const_vec);

    vec.push_back(2, manager.get_allocator());
    test_helper(2, vec);
    test_helper(2, const_vec);

    vec.push_back(3, manager.get_allocator());
    test_helper(3, vec);
    test_helper(3, const_vec);

    vec.destroy(manager.get_allocator());
    EXPECT_EQ(vec.size(), 0);
    EXPECT_EQ(const_vec.size(), 0);
  }
}

TEST(CompactVectorTest, Capacity) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    vec_type    vec;
    const auto& const_vec = vec;

    EXPECT_EQ(vec.capacity(), 0);
    EXPECT_EQ(const_vec.capacity(), 0);

    vec.push_back(1, manager.get_allocator());
    EXPECT_GE(vec.capacity(), 1);
    EXPECT_GE(const_vec.capacity(), 1);

    vec.push_back(2, manager.get_allocator());
    EXPECT_GE(vec.capacity(), 2);
    EXPECT_GE(const_vec.capacity(), 2);

    vec.push_back(3, manager.get_allocator());
    EXPECT_GE(vec.capacity(), 3);
    EXPECT_GE(const_vec.capacity(), 3);

    vec.destroy(manager.get_allocator());
  }
}

TEST(CompactVectorTest, Back) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    vec_type    vec;
    const auto& const_vec = vec;

    vec.push_back(10, manager.get_allocator());
    EXPECT_EQ(vec.back(), 10);
    EXPECT_EQ(const_vec.back(), 10);

    vec.push_back(20, manager.get_allocator());
    EXPECT_EQ(vec.back(), 20);
    EXPECT_EQ(const_vec.back(), 20);

    vec.push_back(30, manager.get_allocator());
    EXPECT_EQ(vec.back(), 30);
    EXPECT_EQ(const_vec.back(), 30);

    vec.destroy(manager.get_allocator());
  }
}

TEST(CompactVectorTest, Resize) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    vec_type vec;

    vec.push_back(100, manager.get_allocator());

    // grow
    vec.resize(10, manager.get_allocator());
    EXPECT_EQ(vec.size(), 10);

    // resize() must preserve existing elements
    EXPECT_EQ(vec[0], 100);

    for (std::size_t i = 0; i < 10; ++i) {
      vec[i] = i;
    }

    for (std::size_t i = 0; i < 10; ++i) {
      EXPECT_EQ(vec[i], i);
    }

    // shrink
    const auto cap = vec.capacity();
    vec.resize(5, manager.get_allocator());
    EXPECT_EQ(vec.size(), 5);
    // capacity must not change
    EXPECT_EQ(vec.capacity(), cap);

    for (std::size_t i = 0; i < 5; ++i) {
      EXPECT_EQ(vec[i], i);
    }

    vec.resize(0, manager.get_allocator());
    EXPECT_EQ(vec.size(), 0);

    vec.destroy(manager.get_allocator());
  }
}

TEST(CompactVectorTest, Clear) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    vec_type vec;

    vec.push_back(10, manager.get_allocator());
    vec.push_back(20, manager.get_allocator());
    vec.push_back(30, manager.get_allocator());

    vec.clear(manager.get_allocator());
    EXPECT_EQ(vec.size(), 0);

    vec.destroy(manager.get_allocator());
  }
}

TEST(CompactVectorTest, ShrinkToFit) {
  metall::manager manager(metall::create_only, "/tmp/metall-test");

  {
    vec_type vec;

    vec.push_back(10, manager.get_allocator());
    vec.push_back(20, manager.get_allocator());
    const auto cap = vec.capacity();

    vec.shrink_to_fit(manager.get_allocator());
    EXPECT_EQ(vec.size(), 2);
    EXPECT_EQ(vec.capacity(), cap);
    EXPECT_EQ(vec[0], 10);
    EXPECT_EQ(vec[1], 20);

    vec.resize(1, manager.get_allocator());
    vec.shrink_to_fit(manager.get_allocator());
    EXPECT_EQ(vec.capacity(), 1);
    EXPECT_EQ(vec[0], 10);

    vec.clear(manager.get_allocator());
    vec.shrink_to_fit(manager.get_allocator());
    EXPECT_EQ(vec.capacity(), 0);

    // Can reuse the vector after shrink_to_fit()
    vec.push_back(100, manager.get_allocator());
    vec.push_back(200, manager.get_allocator());
    EXPECT_EQ(vec.capacity(), 2);
    EXPECT_EQ(vec[0], 100);
    EXPECT_EQ(vec[1], 200);

    vec.destroy(manager.get_allocator());
  }
}