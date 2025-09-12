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

// Helper to validate that variant type matches expected column type
bool validate_variant_type(const metall_series_type& value,
                           Metall_Type               expected_type) {
  switch (expected_type) {
    case Metall_Type::Bool:
      return std::holds_alternative<bool>(value) ||
             std::holds_alternative<std::monostate>(value);
    case Metall_Type::Int64:
      return std::holds_alternative<int64_t>(value) ||
             std::holds_alternative<std::monostate>(value);
    case Metall_Type::UInt64:
      return std::holds_alternative<uint64_t>(value) ||
             std::holds_alternative<std::monostate>(value);
    case Metall_Type::Double:
      return std::holds_alternative<double>(value) ||
             std::holds_alternative<std::monostate>(value);
    case Metall_Type::String:
      return std::holds_alternative<std::string_view>(value) ||
             std::holds_alternative<std::monostate>(value);
  }
  return false;
}

// Simple helper to append a value to the appropriate builder type
arrow::Status append_value_to_builder(arrow::ArrayBuilder*      builder,
                                      const metall_series_type& value) {
  return std::visit(
      [builder](const auto& val) -> arrow::Status {
        using T = std::decay_t<decltype(val)>;

        if constexpr (std::is_same_v<T, std::monostate>) {
          return builder->AppendNull();
        } else if constexpr (std::is_same_v<T, bool>) {
          return static_cast<arrow::BooleanBuilder*>(builder)->Append(val);
        } else if constexpr (std::is_same_v<T, int64_t>) {
          return static_cast<arrow::Int64Builder*>(builder)->Append(val);
        } else if constexpr (std::is_same_v<T, uint64_t>) {
          return static_cast<arrow::UInt64Builder*>(builder)->Append(val);
        } else if constexpr (std::is_same_v<T, double>) {
          return static_cast<arrow::DoubleBuilder*>(builder)->Append(val);
        } else if constexpr (std::is_same_v<T, std::string_view>) {
          return static_cast<arrow::StringBuilder*>(builder)->Append(
              val.data(), val.size());
        } else {
          return arrow::Status::Invalid("Unsupported variant type");
        }
      },
      value);
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
                             char delimiter, size_t batch_size)
    : filename_(filename), batch_size_(batch_size), is_valid_(false) {
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

    // Initialize builders for each column
    column_builders_.reserve(field_names_.size());
    for (size_t i = 0; i < field_names_.size(); ++i) {
      Metall_Type field_type = field_types_[i];
      switch (field_type) {
        case Metall_Type::Bool:
          column_builders_.emplace_back(
              std::make_unique<arrow::BooleanBuilder>());
          break;
        case Metall_Type::Int64:
          column_builders_.emplace_back(
              std::make_unique<arrow::Int64Builder>());
          break;
        case Metall_Type::UInt64:
          column_builders_.emplace_back(
              std::make_unique<arrow::UInt64Builder>());
          break;
        case Metall_Type::Double:
          column_builders_.emplace_back(
              std::make_unique<arrow::DoubleBuilder>());
          break;
        case Metall_Type::String:
          column_builders_.emplace_back(
              std::make_unique<arrow::StringBuilder>());
          break;
      }
    }

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
                             char delimeter, size_t batch_size)
    : ParquetWriter::ParquetWriter(filename,
                                   parse_field_types_str(fields_with_type_str),
                                   delimeter, batch_size) {};

ParquetWriter::ParquetWriter(ParquetWriter&& other) noexcept
    : filename_(std::move(other.filename_)),
      field_names_(std::move(other.field_names_)),
      field_types_(std::move(other.field_types_)),
      schema_(std::move(other.schema_)),
      outfile_(std::move(other.outfile_)),
      writer_(std::move(other.writer_)),
      column_builders_(std::move(other.column_builders_)),
      batch_size_(other.batch_size_),
      is_valid_(other.is_valid_) {
  other.is_valid_ = false;
}

ParquetWriter& ParquetWriter::operator=(ParquetWriter&& other) noexcept {
  if (this != &other) {
    // Close current file if open
    if (is_valid_) {
      (void)close();  // Ignore return value in destructor
    }

    filename_        = std::move(other.filename_);
    field_names_     = std::move(other.field_names_);
    field_types_     = std::move(other.field_types_);
    schema_          = std::move(other.schema_);
    outfile_         = std::move(other.outfile_);
    writer_          = std::move(other.writer_);
    column_builders_ = std::move(other.column_builders_);
    batch_size_      = other.batch_size_;
    is_valid_        = other.is_valid_;

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

size_t ParquetWriter::get_batch_size() const { return batch_size_; }

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

arrow::Status ParquetWriter::write_rows(
    const std::vector<std::vector<metall_series_type>>& rows) {
  if (!is_valid_) {
    return arrow::Status::Invalid("Writer is not valid");
  }

  if (rows.empty()) {
    return arrow::Status::OK();
  }

  // Write each row using the efficient write_row method
  for (const auto& row : rows) {
    ARROW_RETURN_NOT_OK(write_row(row));
  }

  return arrow::Status::OK();
}

arrow::Status ParquetWriter::write_row(
    const std::vector<metall_series_type>& row) {
  if (!is_valid_) {
    return arrow::Status::Invalid("ParquetWriter is not valid");
  }

  if (row.size() != field_names_.size()) {
    return arrow::Status::Invalid(
        "Row size (" + std::to_string(row.size()) +
        ") does not match expected number of fields (" +
        std::to_string(field_names_.size()) + ")");
  }

  // Append each column value directly to its builder
  for (size_t col = 0; col < row.size(); ++col) {
    const auto& value         = row[col];
    Metall_Type expected_type = field_types_[col];
    auto*       builder       = column_builders_[col].get();

    // Validate that variant type matches expected column type
    if (!validate_variant_type(value, expected_type)) {
      return arrow::Status::Invalid(
          "Type mismatch in field '" + field_names_[col] +
          "': variant type doesn't match expected column type");
    }

    auto status = append_value_to_builder(builder, value);
    if (!status.ok()) {
      return arrow::Status::Invalid("Error in field '" + field_names_[col] +
                                    "': " + status.message());
    }
  }

  // Check if the first builder has reached the batch size and flush if so
  if (column_builders_[0]->length() >= static_cast<int64_t>(batch_size_)) {
    return flush();
  }

  return arrow::Status::OK();
}

arrow::Status ParquetWriter::flush() {
  if (!is_valid_) {
    return arrow::Status::Invalid("ParquetWriter is not valid");
  }

  // Check if there's anything to flush
  if (column_builders_.empty() || column_builders_[0]->length() == 0) {
    return arrow::Status::OK();
  }

  // Build arrays from the current builder state
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(column_builders_.size());

  for (auto& builder : column_builders_) {
    std::shared_ptr<arrow::Array> array;
    ARROW_ASSIGN_OR_RAISE(array, builder->Finish());
    arrays.push_back(array);
  }

  // Create table and write it as a row group to the Parquet file
  int64_t num_rows = arrays.empty() ? 0 : arrays[0]->length();
  auto    table    = arrow::Table::Make(schema_, arrays, num_rows);
  ARROW_RETURN_NOT_OK(writer_->WriteTable(*table));

  // Reset all builders for the next batch
  for (auto& builder : column_builders_) {
    builder->Reset();
  }

  return arrow::Status::OK();
}

arrow::Status ParquetWriter::close() {
  if (!is_valid_) {
    return arrow::Status::OK();
  }

  // Flush any remaining rows in the batch
  auto flush_status = flush();
  if (!flush_status.ok()) {
    return flush_status;
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
