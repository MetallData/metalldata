// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#include <gtest/gtest.h>

#include <multiseries/multiseries_record.hpp>

using namespace multiseries;

TEST(MultiSeriesTest, Basic) {
  record_store::string_store_type string_store;
  record_store store(&string_store);

  [[maybe_unused]] const auto name_series =
      store.add_series<std::string_view>("name");
  [[maybe_unused]] const auto age_series = store.add_series<uint64_t>("age");
  [[maybe_unused]] const auto city_series =
      store.add_series<std::string_view>("city");

  std::vector<std::string_view> names = {"Alice", "Bob", "Charlie", "David",
                                         "Eve"};
  std::vector<uint64_t> ages = {20, 30, 40, 50, 60};
  std::vector<std::string_view> cities = {"New York", "Los Angeles", "Chicago",
                                          "New York", "Chicago"};
  for (size_t i = 0; i < cities.size(); ++i) {
    const auto record_id = store.add_record();
    EXPECT_TRUE(store.is_none(name_series, record_id));
    store.set<std::string_view>(name_series, record_id, names[i]);
    EXPECT_FALSE(store.is_none(name_series, record_id));

    EXPECT_TRUE(store.is_none("age", record_id));
    store.set<uint64_t>("age", record_id, ages[i]);
    EXPECT_FALSE(store.is_none("age", record_id));

    EXPECT_TRUE(store.is_none("city", record_id));
    store.set<std::string_view>("city", record_id, cities[i]);
    EXPECT_FALSE(store.is_none("city", record_id));
  }

  // Check values
  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_EQ(store.get<std::string_view>("name", i), names[i]);
    EXPECT_EQ(store.get<uint64_t>("age", i), ages[i]);
    EXPECT_EQ(store.get<std::string_view>(city_series, i), cities[i]);
  }
}