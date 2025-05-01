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
  [[maybe_unused]] const auto flag_series =
    store.add_series<bool>("flag");

  std::vector<std::string_view> names = {
    "Alice", "Bob", "Charlie", "David",
    "Eve"
  };
  std::vector<uint64_t> ages = {20, 30, 40, 50, 60};
  std::vector<std::string_view> cities = {
    "New York", "Los Angeles", "Chicago",
    "New York", "Chicago"
  };
  std::vector<bool> flags = {true, false, true, false, true};

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

    EXPECT_TRUE(store.is_none("flag", record_id));
    store.set<bool>("flag", record_id, flags[i]);
    EXPECT_FALSE(store.is_none("flag", record_id));
  }

  // Check values
  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_EQ(store.get<std::string_view>("name", i), names[i]);
    EXPECT_EQ(store.get<uint64_t>("age", i), ages[i]);
    EXPECT_EQ(store.get<std::string_view>(city_series, i), cities[i]);
    EXPECT_EQ(store.get<bool>("flag", i), flags[i]);
  }

  // Test for_all_dynamic
  {
    store.for_all_dynamic("age",
                          [&](const auto record_id, const auto value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, uint64_t>) {
                              EXPECT_EQ(value, ages[record_id]);
                            } else {
                              FAIL() << "Unexpected type";
                            }
                          });

    store.for_all_dynamic("city",
                          [&](const auto record_id, const auto value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, std::string_view>) {
                              EXPECT_EQ(value, cities[record_id]);
                            } else {
                              FAIL() << "Unexpected type";
                            }
                          });
  }

  // Test convert
  {
    store.convert("name", multiseries::container_kind::sparse);
    store.convert("age", multiseries::container_kind::sparse);
    store.convert("city", multiseries::container_kind::sparse);

    for (size_t i = 0; i < cities.size(); ++i) {
      EXPECT_EQ(store.get<std::string_view>("name", i), names[i]);
      EXPECT_EQ(store.get<uint64_t>("age", i), ages[i]);
      EXPECT_EQ(store.get<std::string_view>("city", i), cities[i]);
    }

    store.convert("name", multiseries::container_kind::dense);
    store.convert("age", multiseries::container_kind::dense);
    store.convert("city", multiseries::container_kind::dense);

    for (size_t i = 0; i < cities.size(); ++i) {
      EXPECT_EQ(store.get<std::string_view>("name", i), names[i]);
      EXPECT_EQ(store.get<uint64_t>("age", i), ages[i]);
      EXPECT_EQ(store.get<std::string_view>("city", i), cities[i]);
    }
  }

  // Remove series
  {
    store.remove_series("name");
    EXPECT_FALSE(store.contains("name"));
    EXPECT_TRUE(store.contains("age"));
    EXPECT_TRUE(store.contains("city"));
    EXPECT_TRUE(store.contains("flag"));
    EXPECT_EQ(store.num_series(), 3);
  }

  // Remove record
  {
    store.remove_record(0);
    EXPECT_TRUE(store.is_none("name", 0));
    EXPECT_TRUE(store.is_none("age", 0));
    EXPECT_TRUE(store.is_none("city", 0));
    EXPECT_TRUE(store.is_none("flag", 0));
  }

  std::cout << store.load_factor("age") << std::endl;
}
