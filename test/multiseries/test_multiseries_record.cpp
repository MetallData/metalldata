// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#include <gtest/gtest.h>
#include <multiseries/multiseries_record.hpp>
#include <unordered_map>

using namespace multiseries;

// Global value vectors
std::vector<std::string_view> names  = {"Alice", "Bob", "Charlie", "David",
                                        "Eve"};
std::vector<uint64_t>         ages   = {20, 30, 40, 50, 60};
std::vector<std::string_view> cities = {"New York", "Los Angeles", "Chicago",
                                        "New York", "Chicago"};
std::vector<bool>             flags  = {true, false, true, false, true};

// Helper function to initialize the record store and return a map of series
// names to indices
std::unordered_map<std::string, size_t> initialize_store(record_store& store) {
  std::unordered_map<std::string, size_t> series_indices;

  series_indices["name"] = store.add_series<std::string_view>("name");
  series_indices["age"]  = store.add_series<uint64_t>("age");
  series_indices["city"] = store.add_series<std::string_view>("city");
  series_indices["flag"] = store.add_series<bool>("flag");

  for (size_t i = 0; i < cities.size(); ++i) {
    const auto record_id = store.add_record();
    store.set<std::string_view>(series_indices["name"], record_id, names[i]);
    store.set<uint64_t>(series_indices["age"], record_id, ages[i]);
    store.set<std::string_view>(series_indices["city"], record_id, cities[i]);
    store.set<bool>(series_indices["flag"], record_id, flags[i]);
  }

  return series_indices;
}

TEST(MultiSeriesTest, GetValues) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto series_indices = initialize_store(store);

  // Use series indices to retrieve values
  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_EQ(store.get<std::string_view>(series_indices["name"], i), names[i]);
    EXPECT_EQ(store.get<uint64_t>(series_indices["age"], i), ages[i]);
    EXPECT_EQ(store.get<std::string_view>(series_indices["city"], i),
              cities[i]);
    EXPECT_EQ(store.get<bool>(series_indices["flag"], i), flags[i]);
  }

  // Use series names to retrieve values
  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_EQ(store.get<std::string_view>(series_indices["name"], i), names[i]);
    EXPECT_EQ(store.get<uint64_t>(series_indices["age"], i), ages[i]);
    EXPECT_EQ(store.get<std::string_view>(series_indices["city"], i),
              cities[i]);
    EXPECT_EQ(store.get<bool>(series_indices["flag"], i), flags[i]);
  }
}

TEST(MultiSeriesTest, ContainsSeries) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto series_indices = initialize_store(store);

  EXPECT_TRUE(store.contains_series(series_indices["name"]));
  EXPECT_TRUE(store.contains_series(series_indices["age"]));
  EXPECT_TRUE(store.contains_series(series_indices["city"]));
  EXPECT_TRUE(store.contains_series(series_indices["flag"]));
  EXPECT_FALSE(store.contains_series(series_indices.size()));

  // Use series names
  EXPECT_TRUE(store.contains_series("name"));
  EXPECT_TRUE(store.contains_series("age"));
  EXPECT_TRUE(store.contains_series("city"));
  EXPECT_TRUE(store.contains_series("flag"));
  EXPECT_FALSE(store.contains_series("non_existent_series"));
}

// Test contains_record
TEST(MultiSeriesTest, ContainsRecord) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto series_indices = initialize_store(store);

  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_TRUE(store.contains_record(i));
  }
  EXPECT_FALSE(store.contains_record(cities.size()));
}

// Is_none
TEST(MultiSeriesTest, IsNone) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto name_id = store.find_series("name");
  EXPECT_TRUE(store.is_none(name_id, 0));
  store.add_series<std::string_view>("name");
  name_id = store.find_series("name");
  EXPECT_TRUE(store.is_none(name_id, 0));
  store.add_record();
  EXPECT_TRUE(store.is_none(name_id, 0));
  store.set<std::string_view>(name_id, 0, "Alice");
  EXPECT_FALSE(store.is_none(name_id, 0));
}

// remove_data
TEST(MultiSeriesTest, RemoveData) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto name_idx = store.add_series<std::string_view>("name");
  EXPECT_FALSE(store.remove(name_idx, 0));

  store.add_record();
  EXPECT_FALSE(store.remove(name_idx, 0));

  store.set<std::string_view>(name_idx, 0, "Alice");
  EXPECT_TRUE(store.remove(name_idx, 0));
  EXPECT_TRUE(store.is_none(name_idx, 0));
}

TEST(MultiSeriesTest, SeriesTypeChecks) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto series_indices = initialize_store(store);

  EXPECT_TRUE(store.is_series_type<std::string_view>(series_indices["name"]));
  EXPECT_TRUE(store.is_series_type<uint64_t>(series_indices["age"]));
  EXPECT_TRUE(store.is_series_type<std::string_view>(series_indices["city"]));
  EXPECT_TRUE(store.is_series_type<bool>(series_indices["flag"]));

  EXPECT_FALSE(store.is_series_type<int64_t>(series_indices["name"]));
  EXPECT_FALSE(store.is_series_type<double>(series_indices["age"]));
  EXPECT_FALSE(store.is_series_type<int64_t>(series_indices["city"]));
  EXPECT_FALSE(store.is_series_type<std::string_view>(series_indices["flag"]));
}

TEST(MultiSeriesTest, ForAllDynamic) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  initialize_store(store);

  store.for_all_dynamic("age", [&](const auto record_id, const auto value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, uint64_t>) {
      EXPECT_EQ(value, ages[record_id]);
    } else {
      FAIL() << "Unexpected type";
    }
  });

  store.for_all_dynamic("city", [&](const auto record_id, const auto value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, std::string_view>) {
      EXPECT_EQ(value, cities[record_id]);
    } else {
      FAIL() << "Unexpected type";
    }
  });
}

TEST(MultiSeriesTest, ConvertAndCheck) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto series_indices = initialize_store(store);

  store.convert(series_indices["name"], multiseries::container_kind::sparse);
  store.convert(series_indices["age"], multiseries::container_kind::sparse);
  store.convert(series_indices["city"], multiseries::container_kind::sparse);

  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_EQ(store.get<std::string_view>(series_indices["name"], i), names[i]);
    EXPECT_EQ(store.get<uint64_t>(series_indices["age"], i), ages[i]);
    EXPECT_EQ(store.get<std::string_view>(series_indices["city"], i),
              cities[i]);
  }

  store.convert(series_indices["name"], multiseries::container_kind::dense);
  store.convert(series_indices["age"], multiseries::container_kind::dense);
  store.convert(series_indices["city"], multiseries::container_kind::dense);

  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_EQ(store.get<std::string_view>(series_indices["name"], i), names[i]);
    EXPECT_EQ(store.get<uint64_t>(series_indices["age"], i), ages[i]);
    EXPECT_EQ(store.get<std::string_view>(series_indices["city"], i),
              cities[i]);
  }
}

TEST(MultiSeriesTest, RemoveSeriesAndRecords) {
  record_store::string_store_type string_store;
  record_store                    store(&string_store);

  auto series_indices = initialize_store(store);

  store.remove_series(series_indices["name"]);
  EXPECT_FALSE(store.contains_series("name"));
  EXPECT_EQ(store.num_series(), 3);

  store.remove_record(0);
  EXPECT_TRUE(store.is_none(series_indices["age"], 0));
  EXPECT_TRUE(store.is_none(series_indices["city"], 0));
  EXPECT_TRUE(store.is_none(series_indices["flag"], 0));
  EXPECT_EQ(store.num_series(), 3);
}