#include "parquet_writer.hpp"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace parquet_writer;

// Test function 1: Basic field specification functionality
void test_field_specification() {
  std::cout << "Testing field specification functionality..." << std::endl;

  std::vector<std::string> field_specs = {
      "id:i",     // int64_t
      "count:u",  // uint64_t
      "value:f",  // double/float
      "name:s",   // string
      "flag:b"    // bool
  };

  try {
    ParquetWriter writer("test_field_specs.parquet", field_specs);
    assert(writer.IsValid());

    // Write a test row
    auto status =
        writer.write_row(int64_t(42), uint64_t(100), 3.14, "test", true);
    assert(status.ok());

    std::cout << "✓ Field specification test passed" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ Field specification test failed: " << e.what() << std::endl;
    assert(false);
  }
}

// Test function 2: WriteParquet with vector
void test_write_parquet_vector() {
  std::cout << "Testing WriteParquet with vector..." << std::endl;

  std::vector<std::string> field_specs = {"id:i", "value:f", "flag:b"};

  try {
    ParquetWriter writer("test_vector.parquet", field_specs);
    assert(writer.IsValid());

    std::vector<metall_series_type> row = {int64_t(123), 2.718, true};

    auto status = writer.write_row(row);
    assert(status.ok());

    std::cout << "✓ WriteParquet vector test passed" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ WriteParquet vector test failed: " << e.what() << std::endl;
    assert(false);
  }
}

// Test function 3: Null handling with std::monostate
void test_null_handling() {
  std::cout << "Testing null handling..." << std::endl;

  std::vector<std::string> field_specs = {"id:i", "nullable_value:f", "flag:b"};

  try {
    ParquetWriter writer("test_nulls.parquet", field_specs);
    assert(writer.IsValid());

    // Test row with null (std::monostate)
    std::vector<metall_series_type> row_with_null = {int64_t(456),
                                                     std::monostate{}, false};

    auto status = writer.write_row(row_with_null);
    assert(status.ok());

    std::cout << "✓ Null handling test passed" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ Null handling test failed: " << e.what() << std::endl;
    assert(false);
  }
}

// Test function 4: WriteDataFrameToParquet utility function
void test_dataframe_to_parquet() {
  std::cout << "Testing single string field spec..." << std::endl;

  std::vector<std::vector<metall_series_type>> dataframe = {
      {int64_t(1), 1.1, true},
      {int64_t(2), 2.2, false},
      {int64_t(3), 3.3, true}};

  std::string field_specs = {"id:i,value:f,flag:b"};

  auto writer = ParquetWriter("test_dataframe.parquet", field_specs);
  auto status = writer.write_rows(dataframe);
  assert(status.ok());

  std::cout << "✓ single string field spec test passed" << std::endl;
}

// Cleanup function to remove test files
void cleanup_test_files() {
  const std::vector<std::string> test_files = {
      "test_field_specs.parquet", "test_vector.parquet", "test_nulls.parquet",
      "test_dataframe.parquet", "test_optional_nulls.parquet"};

  for (const auto& file : test_files) {
    try {
      if (std::filesystem::exists(file)) {
        std::filesystem::remove(file);
      }
    } catch (const std::exception& e) {
      // Ignore cleanup errors
    }
  }
}

int main() {
  std::cout << "Running ParquetWriter tests..." << std::endl;

  try {
    // test_field_specification();
    test_write_parquet_vector();
    test_null_handling();
    test_dataframe_to_parquet();
    // test_write_row_with_nulls();

    std::cout << "\n✓ All tests passed!" << std::endl;

    // Clean up test files
    cleanup_test_files();

    return 0;
  } catch (const std::exception& e) {
    std::cerr << "\n✗ Test suite failed: " << e.what() << std::endl;
    cleanup_test_files();
    return 1;
  }
}
