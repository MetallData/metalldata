#include "parquet_writer.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <iostream>

namespace parquet_writer {

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

// Simple helper to append a value to the appropriate builder type
arrow::Status append_value_to_builder(arrow::ArrayBuilder*      builder,
                                      const metall_series_type& value,
                                      Metall_Type               expected_type) {
  if (std::holds_alternative<std::monostate>(value)) {
    return builder->AppendNull();
  }

  switch (expected_type) {
    case Metall_Type::Bool:
      if (std::holds_alternative<bool>(value)) {
        return static_cast<arrow::BooleanBuilder*>(builder)->Append(
            std::get<bool>(value));
      }
      break;
    case Metall_Type::Int64:
      if (std::holds_alternative<int64_t>(value)) {
        return static_cast<arrow::Int64Builder*>(builder)->Append(
            std::get<int64_t>(value));
      }
      break;
    case Metall_Type::UInt64:
      if (std::holds_alternative<uint64_t>(value)) {
        return static_cast<arrow::UInt64Builder*>(builder)->Append(
            std::get<uint64_t>(value));
      }
      break;
    case Metall_Type::Double:
      if (std::holds_alternative<double>(value)) {
        return static_cast<arrow::DoubleBuilder*>(builder)->Append(
            std::get<double>(value));
      }
      break;
    case Metall_Type::String:
      if (std::holds_alternative<std::string_view>(value)) {
        auto sv = std::get<std::string_view>(value);
        return static_cast<arrow::StringBuilder*>(builder)->Append(sv.data(),
                                                                   sv.size());
      }
      break;
  }
  return arrow::Status::Invalid(
      "Type mismatch - variant type doesn't match expected builder type");
}

std::pair<std::vector<std::string>, name_to_type> parse_field_types(
    const std::vector<std::string>& fields_with_type, char delimiter) {
  name_to_type             ntt{};
  std::vector<std::string> field_list{};
  field_list.reserve(fields_with_type.size());

  for (const auto& field_with_type : fields_with_type) {
    size_t n = field_with_type.size();
    if (n < 3) {
      throw InvalidFieldSpecError(field_with_type);
    }
    if (field_with_type[n - 2] != delimiter) {
      throw DelimiterNotFoundError(field_with_type, delimiter);
    }
    auto field_name = field_with_type.substr(0, n - 2);
    auto type_name  = field_with_type[n - 1];
    if (!char_to_type.contains(type_name)) {
      throw InvalidTypeError(type_name);
    }
    if (ntt.contains(field_name)) {
      throw DuplicateFieldError(field_name);
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
  try {
    // Parse the field names and types using the existing function
    auto [field_names, name_type_map] =
        parse_field_types(fields_with_type, delimiter);

    field_names_ = std::move(field_names);

    // Pre-compute field types in order to avoid map lookups in hot paths
    field_types_.reserve(field_names_.size());
    for (const auto& field_name : field_names_) {
      field_types_.push_back(name_type_map[field_name]);
    }

    // Initialize builders for all fields
    type_builders_.emplace(Metall_Type::Bool,
                           std::make_unique<arrow::BooleanBuilder>());
    type_builders_.emplace(Metall_Type::Int64,
                           std::make_unique<arrow::Int64Builder>());
    type_builders_.emplace(Metall_Type::UInt64,
                           std::make_unique<arrow::UInt64Builder>());
    type_builders_.emplace(Metall_Type::Double,
                           std::make_unique<arrow::DoubleBuilder>());
    type_builders_.emplace(Metall_Type::String,
                           std::make_unique<arrow::StringBuilder>());

    auto status = initialize();
    if (!status.ok()) {
      is_valid_ = false;
    }
  } catch (const ParseError& e) {
    // Log the parse error but don't terminate the program
    std::cerr << "ParquetWriter constructor error: " << e.what() << std::endl;
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
      field_types_(std::move(other.field_types_)),
      schema_(std::move(other.schema_)),
      outfile_(std::move(other.outfile_)),
      writer_(std::move(other.writer_)),
      type_builders_(std::move(other.type_builders_)),
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
    field_types_   = std::move(other.field_types_);
    schema_       = std::move(other.schema_);
    outfile_      = std::move(other.outfile_);
    writer_       = std::move(other.writer_);
    type_builders_ = std::move(other.type_builders_);
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

  for (size_t i = 0; i < field_names_.size(); ++i) {
    const auto& field_name = field_names_[i];

    // Use pre-computed field type instead of map lookup
    Metall_Type field_type = field_types_[i];

    auto arrow_type_it = metall_to_arrow_type.find(field_type);
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

    // Use pre-computed field type instead of map lookup
    Metall_Type expected_type = field_types_[i];

    // Get reusable builder and reset it
    auto* builder = type_builders_.at(expected_type).get();
    builder->Reset();

    std::shared_ptr<arrow::Array> array;

    // Use simple helper function
    auto status = append_value_to_builder(builder, value, expected_type);
    if (!status.ok()) {
      return arrow::Status::Invalid("Error in field '" + field_name +
                                    "': " + status.message());
    }
    ARROW_ASSIGN_OR_RAISE(array, builder->Finish());

    arrays.push_back(array);
  }

  auto table = arrow::Table::Make(schema_, arrays, 1);
  ARROW_RETURN_NOT_OK(writer_->WriteTable(*table));

  return arrow::Status::OK();
}

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

    Metall_Type expected_type = field_types_[col];

    std::shared_ptr<arrow::Array> array;

    // Get the existing builder and reset it
    auto* builder = type_builders_.at(expected_type).get();
    builder->Reset();
    ARROW_RETURN_NOT_OK(builder->Reserve(num_rows));

    // Process all rows for this column using the same helper
    for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
      auto status =
          append_value_to_builder(builder, rows[row_idx][col], expected_type);
      if (!status.ok()) {
        return arrow::Status::Invalid("Error in field '" + field_name +
                                      "' at row " + std::to_string(row_idx) +
                                      ": " + status.message());
      }
    }

    ARROW_ASSIGN_OR_RAISE(array, builder->Finish());

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

}  // namespace parquet_writer
