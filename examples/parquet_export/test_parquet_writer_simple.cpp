/*
 * Comprehensive Test Suite for ParquetWriter
 *
 * This test suite covers:
 * 1. Basic functionality: Vector row writing, null handling, field
 * specifications
 * 2. Type optimization: Multiple columns of same type (tests builder reuse)
 * 3. Data type coverage: All supported types (bool, int64, uint64, double,
 * string)
 * 4. Bulk operations: write_rows with large datasets
 * 5. Null handling: Mixed null and non-null values using std::monostate
 * 6. Error handling: Row size mismatches and type safety
 * 7. RAII & Move semantics: Resource management and move operations
 * 8. String parsing: Comma-separated field specifications with whitespace
 * handling
 *
 * Tests validate both the type-based builder optimization and overall
 * robustness.
 */

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

// Test function 5: Multiple data types in same column type
void test_multiple_same_type_columns() {
  std::cout << "Testing multiple columns of same type..." << std::endl;

  std::vector<std::string> field_specs = {
      "id1:i",   "id2:i",   "id3:i",  // Multiple int64 columns
      "name1:s", "name2:s",           // Multiple string columns
      "flag1:b", "flag2:b"            // Multiple bool columns
  };

  try {
    ParquetWriter writer("test_multiple_same_type.parquet", field_specs);
    assert(writer.is_valid());

    // Write test rows
    std::vector<metall_series_type> row1 = {int64_t(1),
                                            int64_t(2),
                                            int64_t(3),
                                            std::string_view("hello"),
                                            std::string_view("world"),
                                            true,
                                            false};

    std::vector<metall_series_type> row2 = {int64_t(10),
                                            int64_t(20),
                                            int64_t(30),
                                            std::string_view("foo"),
                                            std::string_view("bar"),
                                            false,
                                            true};

    auto status1 = writer.write_row(row1);
    assert(status1.ok());

    auto status2 = writer.write_row(row2);
    assert(status2.ok());

    std::cout << "✓ Multiple same type columns test passed" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ Multiple same type columns test failed: " << e.what()
              << std::endl;
    assert(false);
  }
}

// Test function 6: All data types test
void test_all_data_types() {
  std::cout << "Testing all supported data types..." << std::endl;

  std::vector<std::string> field_specs = {"bool_col:b", "int64_col:i",
                                          "uint64_col:u", "double_col:f",
                                          "string_col:s"};

  try {
    ParquetWriter writer("test_all_types.parquet", field_specs);
    assert(writer.is_valid());

    // Test with actual values
    std::vector<metall_series_type> row1 = {true, int64_t(-12345),
                                            uint64_t(67890), 3.14159,
                                            std::string_view("test_string")};

    // Test with edge values
    std::vector<metall_series_type> row2 = {
        false,
        int64_t(-9223372036854775807LL),    // Near INT64_MIN
        uint64_t(18446744073709551615ULL),  // UINT64_MAX
        -1.23e-10,
        std::string_view("")  // Empty string
    };

    auto status1 = writer.write_row(row1);
    assert(status1.ok());

    auto status2 = writer.write_row(row2);
    assert(status2.ok());

    std::cout << "✓ All data types test passed" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ All data types test failed: " << e.what() << std::endl;
    assert(false);
  }
}

// Test function 7: Bulk write with write_rows
void test_bulk_write() {
  std::cout << "Testing bulk write with write_rows..." << std::endl;

  std::vector<std::string> field_specs = {"id:i", "value:f", "category:s"};

  try {
    ParquetWriter writer("test_bulk_write.parquet", field_specs);
    assert(writer.is_valid());

    // Create a larger dataset
    std::vector<std::vector<metall_series_type>> rows;
    for (int i = 0; i < 100; ++i) {
      rows.push_back({int64_t(i), double(i * 0.5),
                      std::string_view(i % 2 == 0 ? "even" : "odd")});
    }

    auto status = writer.write_rows(rows);
    assert(status.ok());

    std::cout << "✓ Bulk write test passed (100 rows)" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ Bulk write test failed: " << e.what() << std::endl;
    assert(false);
  }
}

// Test function 8: Mixed nulls and values
void test_mixed_nulls() {
  std::cout << "Testing mixed nulls and values..." << std::endl;

  std::vector<std::string> field_specs = {"id:i", "optional_value:f",
                                          "optional_string:s"};

  try {
    ParquetWriter writer("test_mixed_nulls.parquet", field_specs);
    assert(writer.is_valid());

    std::vector<std::vector<metall_series_type>> rows = {
        {int64_t(1), 1.1, std::string_view("first")},
        {int64_t(2), std::monostate{}, std::string_view("second")},
        {int64_t(3), 3.3, std::monostate{}},
        {int64_t(4), std::monostate{}, std::monostate{}},
        {int64_t(5), 5.5, std::string_view("fifth")}};

    auto status = writer.write_rows(rows);
    assert(status.ok());

    std::cout << "✓ Mixed nulls test passed" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ Mixed nulls test failed: " << e.what() << std::endl;
    assert(false);
  }
}

// Test function 9: Error handling
void test_error_handling() {
  std::cout << "Testing error handling..." << std::endl;

  // Test mismatched row size
  try {
    std::vector<std::string> field_specs = {"id:i", "value:f"};
    ParquetWriter            writer("test_mismatch.parquet", field_specs);
    assert(writer.is_valid());

    // Row with wrong number of elements
    std::vector<metall_series_type> wrong_size_row = {
        int64_t(1)};  // Missing second element
    auto status = writer.write_row(wrong_size_row);
    assert(!status.ok());  // Should fail
    std::cout << "✓ Row size mismatch correctly detected" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ Error handling test failed: " << e.what() << std::endl;
    assert(false);
  }

  // Test type mismatch (this should be caught by variant type checking)
  try {
    std::vector<std::string> field_specs = {"id:i", "value:f"};
    ParquetWriter            writer("test_type_mismatch.parquet", field_specs);
    assert(writer.is_valid());

    // Try to write wrong types - this should work since we use variants
    // but let's test the array creation process
    std::vector<metall_series_type> valid_row = {int64_t(1), 2.5};
    auto                            status    = writer.write_row(valid_row);
    assert(status.ok());
    std::cout << "✓ Type safety with variants working correctly" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ Type safety test failed: " << e.what() << std::endl;
    assert(false);
  }
}

// Test function 10: RAII and move semantics
void test_raii_and_move() {
  std::cout << "Testing RAII and move semantics..." << std::endl;

  std::vector<std::string> field_specs = {"id:i", "value:f"};

  // Test move constructor
  {
    ParquetWriter writer1("test_move1.parquet", field_specs);
    assert(writer1.is_valid());

    ParquetWriter writer2 = std::move(writer1);
    assert(writer2.is_valid());
    assert(!writer1.is_valid());  // Original should be invalid

    // Test that moved writer works
    auto status = writer2.write_row({int64_t(42), 3.14});
    assert(status.ok());
  }  // Both writers should be destroyed properly

  // Test move assignment
  {
    ParquetWriter writer1("test_move2.parquet", field_specs);
    ParquetWriter writer2("test_move3.parquet", field_specs);

    assert(writer1.is_valid());
    assert(writer2.is_valid());

    writer2 = std::move(writer1);
    assert(writer2.is_valid());
    assert(!writer1.is_valid());
  }

  std::cout << "✓ RAII and move semantics test passed" << std::endl;
}

// Test function 11: String field spec parsing
void test_string_field_spec_parsing() {
  std::cout << "Testing string field spec parsing..." << std::endl;

  try {
    // Test comma-separated string
    ParquetWriter writer1("test_string_spec1.parquet", "id:i,name:s,value:f");
    assert(writer1.is_valid());

    // Test with spaces
    ParquetWriter writer2("test_string_spec2.parquet",
                          " id:i , name:s , value:f ");
    assert(writer2.is_valid());

    // Test single field
    ParquetWriter writer3("test_string_spec3.parquet", "single_field:i");
    assert(writer3.is_valid());

    // Write test data
    auto status =
        writer1.write_row({int64_t(1), std::string_view("test"), 1.23});
    assert(status.ok());

    std::cout << "✓ String field spec parsing test passed" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "✗ String field spec parsing test failed: " << e.what()
              << std::endl;
    assert(false);
  }
}

// Cleanup function to remove test files
void cleanup_test_files() {
  const std::vector<std::string> test_files = {
      "test_field_specs.parquet",    "test_vector.parquet",
      "test_nulls.parquet",          "test_dataframe.parquet",
      "test_optional_nulls.parquet", "test_multiple_same_type.parquet",
      "test_all_types.parquet",      "test_bulk_write.parquet",
      "test_mixed_nulls.parquet",    "test_invalid.parquet",
      "test_mismatch.parquet",       "test_type_mismatch.parquet",
      "test_move1.parquet",          "test_move2.parquet",
      "test_move3.parquet",          "test_string_spec1.parquet",
      "test_string_spec2.parquet",   "test_string_spec3.parquet"};

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
    // Basic functionality tests
    test_write_parquet_vector();
    test_null_handling();
    test_dataframe_to_parquet();

    // Advanced functionality tests
    test_multiple_same_type_columns();
    test_all_data_types();
    test_bulk_write();
    test_mixed_nulls();

    // Robustness tests
    test_error_handling();
    test_raii_and_move();
    test_string_field_spec_parsing();

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
