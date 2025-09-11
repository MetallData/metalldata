#pragma once

#include <arrow/status.h>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

// Forward declarations
namespace arrow {
class Schema;
class DataType;
namespace io {
class FileOutputStream;
}
}  // namespace arrow

namespace parquet {
namespace arrow {
class FileWriter;
}
}  // namespace parquet

// Type definitions
using metall_series_type = std::variant<std::monostate, bool, int64_t, uint64_t,
                                        double, std::string_view>;

enum class Metall_Type { Bool, Int64, UInt64, Double, String };

using name_to_type = std::unordered_map<std::string, Metall_Type>;

// Type mappings
extern const std::unordered_map<char, Metall_Type> char_to_type;
extern const std::unordered_map<Metall_Type, std::shared_ptr<arrow::DataType>>
    metall_to_arrow_type;

std::pair<std::vector<std::string>, name_to_type> parse_field_types(
    const std::vector<std::string>& fields_with_type, char delimiter = ':');

class ParquetWriter {
 public:
  // Constructor that uses field specifications
  // Format: field_name:field_type_char
  // where field_type_char is 'b'=bool, 'i'=int64, 'u'=uint64, 'f'=float,
  // 's'=string
  ParquetWriter(const std::string&              filename,
                const std::vector<std::string>& fields_with_type,
                char                            delimiter = ':');

  ParquetWriter(const std::string& filename,
                const std::string& fields_with_type_str, char delimeter = ':');
  // Disable copy constructor and copy assignment to prevent resource
  // duplication
  ParquetWriter(const ParquetWriter&)            = delete;
  ParquetWriter& operator=(const ParquetWriter&) = delete;

  // Enable move constructor and move assignment
  ParquetWriter(ParquetWriter&& other) noexcept;
  ParquetWriter& operator=(ParquetWriter&& other) noexcept;

  ~ParquetWriter();

  bool               is_valid() const;
  const std::string& get_filename() const;

  arrow::Status initialize();
  arrow::Status write_row(const std::vector<metall_series_type>& row);

  // Variadic template overload for write_row - disabled when first arg is a
  // vector
  template <typename T, typename... Args>
  std::enable_if_t<
      !std::is_same_v<std::decay_t<T>, std::vector<metall_series_type>>,
      arrow::Status>
  write_row(T&& first, Args&&... rest) {
    std::vector<metall_series_type> row;
    row.reserve(1 + sizeof...(rest));
    row.emplace_back(std::forward<T>(first));
    (row.emplace_back(std::forward<Args>(rest)), ...);
    return write_row(row);
  }
  arrow::Status write_rows(
      const std::vector<std::vector<metall_series_type>>& rows);
  arrow::Status close();

 private:
  std::string                                  filename_;
  std::vector<std::string>                     field_names_;
  std::unordered_map<std::string, Metall_Type> name_to_type_;
  std::shared_ptr<arrow::Schema>               schema_;
  std::shared_ptr<arrow::io::FileOutputStream> outfile_;
  std::unique_ptr<parquet::arrow::FileWriter>  writer_;
  bool                                         is_valid_;
};
