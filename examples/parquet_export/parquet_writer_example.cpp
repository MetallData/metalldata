#include "parquet_writer.hpp"
#include <iostream>

// Example usage
int main() {
  try {
    // Create sample data - a vector of vectors representing a dataframe
    std::vector<std::vector<metall_series_type>> dataframe = {
        {int64_t(1), uint64_t(100), 3.14, std::string_view("hello"), true},
        {int64_t(2), uint64_t(200), 2.71, std::string_view("world"), false},
        {int64_t(3), uint64_t(300), 1.41, std::string_view("test"), true}};

    // Define columns using field specification strings
    std::vector<std::string> field_specs = {
        "id:i",     // int64_t
        "count:u",  // uint64_t
        "value:f",  // double/float
        "name:s",   // string
        "flag:b"    // bool
    };

    arrow::Status status;
    // Method 1: Using the field specification format
    // auto status = WriteDataFrameToParquet("output_field_specs.parquet",
    //                                       dataframe, field_specs);
    // if (!status.ok()) {
    //   std::cerr << "Failed to write dataframe with field specs: "
    //             << status.ToString() << std::endl;
    //   return 1;
    // }
    // std::cout << "Successfully wrote dataframe using field specs to "
    //              "output_field_specs.parquet"
    //           << std::endl;

    // Method 2: Using ParquetWriter constructor directly with field specs
    try {
      ParquetWriter writer("output_rows_new.parquet", field_specs);

      if (!writer.is_valid()) {
        std::cerr << "Failed to create ParquetWriter with field specs"
                  << std::endl;
        return 1;
      }

      // Write data row by row
      for (const auto& row : dataframe) {
        status = writer.write_row(row);
        if (!status.ok()) {
          std::cerr << "Failed to write row: " << status.ToString()
                    << std::endl;
          return 1;
        }
      }

      std::cout << "Successfully wrote " << dataframe.size()
                << " rows to output_rows_new.parquet" << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Exception creating ParquetWriter with field specs: "
                << e.what() << std::endl;
      return 1;
    }

    // Method 3: Demonstrating WriteRow with field specs
    // try {
    //   ParquetWriter writer2("output_variadic_new.parquet", field_specs);

    //   if (!writer2.is_valid()) {
    //     std::cerr << "Failed to create ParquetWriter for variadic demo"
    //               << std::endl;
    //     return 1;
    //   }

    //   // Write individual rows using the WriteRow method
    //   status =
    //       writer2.WriteRow(int64_t(10), uint64_t(1000), 9.99, "variadic",
    //       true);
    //   if (!status.ok()) {
    //     std::cerr << "Failed to write row with WriteRow: " <<
    //     status.ToString()
    //               << std::endl;
    //     return 1;
    //   }

    //   status =
    //       writer2.WriteRow(int64_t(20), uint64_t(2000), 8.88, "method",
    //       false);
    //   if (!status.ok()) {
    //     std::cerr << "Failed to write row with WriteRow: " <<
    //     status.ToString()
    //               << std::endl;
    //     return 1;
    //   }

    //   std::cout << "Successfully wrote 2 rows using WriteRow method with
    //   field "
    //                "specs to output_variadic_new.parquet"
    //             << std::endl;
    // } catch (const std::exception& e) {
    //   std::cerr << "Exception creating ParquetWriter for variadic demo: "
    //             << e.what() << std::endl;
    //   return 1;
    // }

    // Method 4: Demonstrating WriteRowWithNulls
    // try {
    //   ParquetWriter writer3("output_nulls.parquet", field_specs);

    //   if (!writer3.is_valid()) {
    //     std::cerr << "Failed to create ParquetWriter for nulls demo"
    //               << std::endl;
    //     return 1;
    //   }

    //   // Create a row with some null values
    //   std::vector<std::optional<metall_series_type>> row_with_nulls = {
    //       std::make_optional<metall_series_type>(int64_t(30)),
    //       std::nullopt,  // null value for count column
    //       std::make_optional<metall_series_type>(7.77),
    //       std::make_optional<metall_series_type>(std::string_view("nullable")),
    //       std::make_optional<metall_series_type>(true)};

    //   status = writer3.WriteRowWithNulls(row_with_nulls);
    //   if (!status.ok()) {
    //     std::cerr << "Failed to write row with nulls: " << status.ToString()
    //               << std::endl;
    //     return 1;
    //   }

    //   std::cout
    //       << "Successfully wrote 1 row with null values to
    //       output_nulls.parquet"
    //       << std::endl;
    // } catch (const std::exception& e) {
    //   std::cerr << "Exception creating ParquetWriter for nulls demo: "
    //             << e.what() << std::endl;
    //   return 1;
    // }

    // Method 5: Demonstrating direct std::monostate handling (nulls in
    // metall_series_type)
    try {
      ParquetWriter writer4("output_monostate_nulls.parquet", field_specs);

      if (!writer4.is_valid()) {
        std::cerr << "Failed to create ParquetWriter for monostate demo"
                  << std::endl;
        return 1;
      }

      // Create rows with std::monostate (null) values directly
      std::vector<metall_series_type> row_with_monostate_nulls = {
          int64_t(40),       // id: valid value
          std::monostate{},  // count: null (monostate)
          3.33,              // value: valid value
          std::monostate{},  // name: null (monostate)
          true               // flag: valid value
      };

      status = writer4.write_row(row_with_monostate_nulls);
      if (!status.ok()) {
        std::cerr << "Failed to write row with monostate nulls: "
                  << status.ToString() << std::endl;
        return 1;
      }

      // Add another row with different null pattern
      std::vector<metall_series_type> row_with_more_nulls = {
          std::monostate{},           // id: null (monostate)
          uint64_t(500),              // count: valid value
          std::monostate{},           // value: null (monostate)
          std::string_view("mixed"),  // name: valid value
          std::monostate{}            // flag: null (monostate)
      };

      status = writer4.write_row(row_with_more_nulls);
      if (!status.ok()) {
        std::cerr << "Failed to write second row with monostate nulls: "
                  << status.ToString() << std::endl;
        return 1;
      }

      std::cout << "Successfully wrote 2 rows with std::monostate nulls to "
                   "output_monostate_nulls.parquet"
                << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Exception creating ParquetWriter for monostate demo: "
                << e.what() << std::endl;
      return 1;
    }

    // Method 6: Demonstrating direct std::monostate handling (nulls in
    // metall_series_type) with write_rows
    try {
      ParquetWriter writer6("output_monostate_nulls.parquet", field_specs);

      if (!writer6.is_valid()) {
        std::cerr << "Failed to create ParquetWriter for monostate demo"
                  << std::endl;
        return 1;
      }

      // Create rows with std::monostate (null) values directly
      std::vector<metall_series_type> row1 = {
          int64_t(40),       // id: valid value
          std::monostate{},  // count: null (monostate)
          3.33,              // value: valid value
          std::monostate{},  // name: null (monostate)
          true               // flag: valid value
      };
      std::vector<metall_series_type> row2 = {
          std::monostate{},           // id: null (monostate)
          uint64_t(500),              // count: valid value
          std::monostate{},           // value: null (monostate)
          std::string_view("mixed"),  // name: valid value
          std::monostate{}            // flag: null (monostate)
      };

      std::vector<std::vector<metall_series_type>> rows{row1, row2};

      status = writer6.write_rows(rows);
      if (!status.ok()) {
        std::cerr << "Failed to write row with monostate nulls: "
                  << status.ToString() << std::endl;
        return 1;
      }

      std::cout << "Successfully wrote 2 rows with std::monostate nulls to "
                   "output_monostate_nulls.parquet"
                << std::endl;
    } catch (const std::exception& e) {
      std::cerr
          << "Exception creating ParquetWriter for multi-row monostate demo: "
          << e.what() << std::endl;
      return 1;
    }

  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
