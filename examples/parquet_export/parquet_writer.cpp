#include "parquet_writer.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <iostream>

// Type mappings
const std::unordered_map<char, Metall_Type> char_to_type = {
    {'b', Metall_Type::Bool},
    {'i', Metall_Type::Int64},
    {'u', Metall_Type::UInt64},
    {'f', Metall_Type::Double},
    {'s', Metall_Type::String}};

const std::unordered_map<Metall_Type, std::shared_ptr<arrow::DataType>>
    metall_to_arrow_type = {{Metall_Type::Bool, arrow::boolean()},
                            {Metall_Type::Int64, arrow::int64()},
                            {Metall_Type::UInt64, arrow::uint64()},
                            {Metall_Type::Double, arrow::float64()},
                            {Metall_Type::String, arrow::utf8()}};

std::pair<std::vector<std::string>, name_to_type> parse_field_types(
    const std::vector<std::string>& fields_with_type, char delimiter) {
  name_to_type             ntt{};
  std::vector<std::string> field_list{};
  field_list.reserve(fields_with_type.size());

  for (auto field_with_type : fields_with_type) {
    size_t n = field_with_type.size();
    if (n < 3) {
      std::cerr << "Invalid field name/type designation: " << field_with_type
                << "; aborting\n";
      exit(1);
    }
    if (field_with_type[n - 2] != delimiter) {
      std::cerr << "delimiter not found in " << field_with_type
                << "; aborting\n";
      exit(1);
    }
    auto field_name = field_with_type.substr(0, n - 2);
    auto type_name  = field_with_type[n - 1];
    if (!char_to_type.contains(type_name)) {
      std::cerr << "invalid type name: " << type_name << "; aborting\n";
      exit(1);
    }
    if (ntt.contains(field_name)) {
      std::cerr << "field name already specified; aborting\n";
      exit(1);
    }
    field_list.push_back(field_name);
    ntt[field_name] = char_to_type.at(type_name);
  }
  return {field_list, ntt};
}

// Helper function to split a string by delimiter (internal use only)
static std::vector<std::string> split(const std::string& s,
                                      const std::string& delimiter) {
  std::vector<std::string> tokens;
  size_t                   start = 0;
  size_t                   end   = s.find(delimiter);

  while (end != std::string::npos) {
    tokens.push_back(s.substr(start, end - start));
    start = end + delimiter.length();
    end   = s.find(delimiter, start);
  }
  // Add the last token (or the only token if no delimiter is found)
  tokens.push_back(s.substr(start));
  return tokens;
}

// Parse a single comma-separated string of "field_name:type_char" values
// (internal use only)
static std::vector<std::string> parse_field_types_str(
    const std::string& fields_with_type_str, char field_delimiter = ',',
    char type_delimiter = ':') {
  if (fields_with_type_str.empty()) {
    return {};
  }

  // Split the input string by field_delimiter
  std::string              delimiter_str(1, field_delimiter);
  std::vector<std::string> raw_fields =
      split(fields_with_type_str, delimiter_str);

  std::vector<std::string> fields_with_type;
  fields_with_type.reserve(raw_fields.size());

  for (const auto& field : raw_fields) {
    // Trim whitespace from the field
    size_t start = field.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) continue;  // Skip empty fields

    size_t      end     = field.find_last_not_of(" \t\r\n");
    std::string trimmed = field.substr(start, end - start + 1);

    if (!trimmed.empty()) {
      fields_with_type.push_back(trimmed);
    }
  }

  // Call the existing function with the parsed vector
  return fields_with_type;
}

ParquetWriter::ParquetWriter(const std::string&              filename,
                             const std::vector<std::string>& fields_with_type,
                             char                            delimiter)
    : filename_(filename), is_valid_(false) {
  // Parse the field names and types using the existing function
  auto [field_names, name_type_map] =
      parse_field_types(fields_with_type, delimiter);

  field_names_  = std::move(field_names);
  name_to_type_ = std::move(name_type_map);

  auto status = initialize();
  if (!status.ok()) {
    is_valid_ = false;
  }
}

ParquetWriter::ParquetWriter(const std::string& filename,
                             const std::string& fields_with_type_str,
                             char               delimeter)
    : ParquetWriter::ParquetWriter(
          filename, parse_field_types_str(fields_with_type_str), delimeter) {};

ParquetWriter::ParquetWriter(ParquetWriter&& other) noexcept
    : filename_(std::move(other.filename_)),
      field_names_(std::move(other.field_names_)),
      name_to_type_(std::move(other.name_to_type_)),
      schema_(std::move(other.schema_)),
      outfile_(std::move(other.outfile_)),
      writer_(std::move(other.writer_)),
      is_valid_(other.is_valid_) {
  other.is_valid_ = false;
}

ParquetWriter& ParquetWriter::operator=(ParquetWriter&& other) noexcept {
  if (this != &other) {
    // Close current file if open
    if (is_valid_) {
      (void)close();  // Ignore return value in destructor
    }

    filename_     = std::move(other.filename_);
    field_names_  = std::move(other.field_names_);
    name_to_type_ = std::move(other.name_to_type_);
    schema_       = std::move(other.schema_);
    outfile_      = std::move(other.outfile_);
    writer_       = std::move(other.writer_);
    is_valid_     = other.is_valid_;

    other.is_valid_ = false;
  }
  return *this;
}

ParquetWriter::~ParquetWriter() {
  if (is_valid_) {
    (void)close();  // Ignore return value in destructor
  }
}

bool ParquetWriter::is_valid() const { return is_valid_; }

const std::string& ParquetWriter::get_filename() const { return filename_; }

arrow::Status ParquetWriter::initialize() {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(field_names_.size());

  for (const auto& field_name : field_names_) {
    auto it = name_to_type_.find(field_name);
    if (it == name_to_type_.end()) {
      return arrow::Status::Invalid("Field name not found: " + field_name);
    }

    auto arrow_type_it = metall_to_arrow_type.find(it->second);
    if (arrow_type_it == metall_to_arrow_type.end()) {
      return arrow::Status::Invalid("Unsupported type for field: " +
                                    field_name);
    }

    fields.push_back(arrow::field(field_name, arrow_type_it->second, true));
  }

  schema_ = arrow::schema(fields);

  ARROW_ASSIGN_OR_RAISE(outfile_, arrow::io::FileOutputStream::Open(filename_));

  ARROW_ASSIGN_OR_RAISE(writer_,
                        parquet::arrow::FileWriter::Open(
                            *schema_, arrow::default_memory_pool(), outfile_));

  is_valid_ = true;
  return arrow::Status::OK();
}

arrow::Status ParquetWriter::write_row(
    const std::vector<metall_series_type>& row) {
  if (!is_valid_) {
    return arrow::Status::Invalid("Writer is not valid");
  }

  if (row.size() != field_names_.size()) {
    return arrow::Status::Invalid("Row size does not match number of fields");
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(field_names_.size());

  for (size_t i = 0; i < field_names_.size(); ++i) {
    const auto& field_name = field_names_[i];
    const auto& value      = row[i];

    auto it = name_to_type_.find(field_name);
    if (it == name_to_type_.end()) {
      return arrow::Status::Invalid("Field name not found: " + field_name);
    }

    Metall_Type expected_type = it->second;

    std::shared_ptr<arrow::Array> array;

    // Handle std::monostate (null) case first
    if (std::holds_alternative<std::monostate>(value)) {
      // Create a null array of the appropriate type
      switch (expected_type) {
        case Metall_Type::Bool: {
          arrow::BooleanBuilder builder;
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::Int64: {
          arrow::Int64Builder builder;
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::UInt64: {
          arrow::UInt64Builder builder;
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::Double: {
          arrow::DoubleBuilder builder;
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::String: {
          arrow::StringBuilder builder;
          ARROW_RETURN_NOT_OK(builder.AppendNull());
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
      }
    } else {
      // Handle non-null values
      switch (expected_type) {
        case Metall_Type::Bool: {
          if (!std::holds_alternative<bool>(value)) {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
          arrow::BooleanBuilder builder;
          ARROW_RETURN_NOT_OK(builder.Append(std::get<bool>(value)));
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::Int64: {
          if (!std::holds_alternative<int64_t>(value)) {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
          arrow::Int64Builder builder;
          ARROW_RETURN_NOT_OK(builder.Append(std::get<int64_t>(value)));
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::UInt64: {
          if (!std::holds_alternative<uint64_t>(value)) {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
          arrow::UInt64Builder builder;
          ARROW_RETURN_NOT_OK(builder.Append(std::get<uint64_t>(value)));
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::Double: {
          if (!std::holds_alternative<double>(value)) {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
          arrow::DoubleBuilder builder;
          ARROW_RETURN_NOT_OK(builder.Append(std::get<double>(value)));
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
        case Metall_Type::String: {
          if (!std::holds_alternative<std::string_view>(value)) {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
          arrow::StringBuilder builder;
          auto                 sv = std::get<std::string_view>(value);
          ARROW_RETURN_NOT_OK(builder.Append(sv.data(), sv.size()));
          ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
          break;
        }
      }
    }

    arrays.push_back(array);
  }

  auto table = arrow::Table::Make(schema_, arrays, 1);
  ARROW_RETURN_NOT_OK(writer_->WriteTable(*table));

  return arrow::Status::OK();
}

// arrow::Status ParquetWriter::WriteRowWithNulls(
//     const std::vector<std::optional<metall_series_type>>& row) {
//   std::vector<metall_series_type> converted_row;
//   converted_row.reserve(row.size());

//   for (const auto& opt_value : row) {
//     if (opt_value.has_value()) {
//       converted_row.push_back(opt_value.value());
//     } else {
//       converted_row.push_back(std::monostate{});
//     }
//   }

//   return WriteParquet(converted_row);
// }

arrow::Status ParquetWriter::write_rows(
    const std::vector<std::vector<metall_series_type>>& rows) {
  if (!is_valid_) {
    return arrow::Status::Invalid("Writer is not valid");
  }

  if (rows.empty()) {
    return arrow::Status::OK();
  }

  const size_t num_rows = rows.size();
  const size_t num_cols = field_names_.size();

  // Validate all rows have the correct number of columns
  for (const auto& row : rows) {
    if (row.size() != num_cols) {
      return arrow::Status::Invalid("Row size does not match number of fields");
    }
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(num_cols);

  // Process each column
  for (size_t col = 0; col < num_cols; ++col) {
    const auto& field_name = field_names_[col];

    auto it = name_to_type_.find(field_name);
    if (it == name_to_type_.end()) {
      return arrow::Status::Invalid("Field name not found: " + field_name);
    }

    Metall_Type expected_type = it->second;

    std::shared_ptr<arrow::Array> array;

    switch (expected_type) {
      case Metall_Type::Bool: {
        arrow::BooleanBuilder builder;
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
          const auto& value = rows[row_idx][col];
          if (std::holds_alternative<std::monostate>(value)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else if (std::holds_alternative<bool>(value)) {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<bool>(value)));
          } else {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
        }
        ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
        break;
      }
      case Metall_Type::Int64: {
        arrow::Int64Builder builder;
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
          const auto& value = rows[row_idx][col];
          if (std::holds_alternative<std::monostate>(value)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else if (std::holds_alternative<int64_t>(value)) {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<int64_t>(value)));
          } else {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
        }
        ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
        break;
      }
      case Metall_Type::UInt64: {
        arrow::UInt64Builder builder;
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
          const auto& value = rows[row_idx][col];
          if (std::holds_alternative<std::monostate>(value)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else if (std::holds_alternative<uint64_t>(value)) {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<uint64_t>(value)));
          } else {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
        }
        ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
        break;
      }
      case Metall_Type::Double: {
        arrow::DoubleBuilder builder;
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
          const auto& value = rows[row_idx][col];
          if (std::holds_alternative<std::monostate>(value)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else if (std::holds_alternative<double>(value)) {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<double>(value)));
          } else {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
        }
        ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
        break;
      }
      case Metall_Type::String: {
        arrow::StringBuilder builder;
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
          const auto& value = rows[row_idx][col];
          if (std::holds_alternative<std::monostate>(value)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else if (std::holds_alternative<std::string_view>(value)) {
            auto sv = std::get<std::string_view>(value);
            ARROW_RETURN_NOT_OK(builder.Append(sv.data(), sv.size()));
          } else {
            return arrow::Status::Invalid("Type mismatch for field " +
                                          field_name);
          }
        }
        ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
        break;
      }
    }

    arrays.push_back(array);
  }

  auto table = arrow::Table::Make(schema_, arrays, num_rows);
  ARROW_RETURN_NOT_OK(writer_->WriteTable(*table));

  return arrow::Status::OK();
}

arrow::Status ParquetWriter::close() {
  if (!is_valid_) {
    return arrow::Status::OK();
  }

  arrow::Status status = arrow::Status::OK();

  if (writer_) {
    status = writer_->Close();
    writer_.reset();
  }

  if (outfile_) {
    auto close_status = outfile_->Close();
    if (!close_status.ok() && status.ok()) {
      status = close_status;
    }
    outfile_.reset();
  }

  is_valid_ = false;
  return status;
}

// Utility function to write a dataframe using field specification strings
// arrow::Status WriteDataFrameToParquet(
//     const std::string&                                  filename,
//     const std::vector<std::vector<metall_series_type>>& dataframe,
//     const std::vector<std::string>& field_specs, char delimiter) {
//   try {
//     ParquetWriter writer(filename, field_specs, delimiter);

//     if (!writer.is_valid()) {
//       return arrow::Status::Invalid("Failed to create ParquetWriter");
//     }

//     for (const auto& row : dataframe) {
//       auto status = writer.WriteParquet(row);
//       if (!status.ok()) {
//         return status;
//       }
//     }

//     return arrow::Status::OK();
//   } catch (const std::exception& e) {
//     return arrow::Status::Invalid("Exception in WriteDataFrameToParquet: " +
//                                   std::string(e.what()));
//   }
// }
