#include <string>
#include <map>
#include <expected>
#include <format>

namespace metalldata {

/// Return type for metalldata operations.
///
/// Pairs a std::expected<T, std::string> outcome with a non-fatal warning log.
/// Warnings are keyed by message and counted so repeated warnings accumulate.
///
/// Callers check outcome for success/failure and retrieve the value or error:
/// @code
///   result<size_t> r = some_operation();
///   if (!r.outcome) {
///     std::cerr << r.outcome.error() << "\n";
///   } else {
///     use(r.outcome.value());
///   }
///   for (auto& [msg, n] : r.warnings) {
///     std::cerr << msg << " (" << n << " times)\n";
///   }
/// @endcode
///
/// Implementors set outcome and warnings via the provided methods:
/// @code
///   // error path:
///   result<size_t> r;
///   r.set_error("series {} not found", name);
///   return r;
///
///   // success path (with optional warnings):
///   result<size_t> r;
///   r.add_warning("series {} ignored", name);
///   r.outcome = computed_value;
///   return r;
/// @endcode
template <typename T>
struct result {
  std::map<std::string, size_t> warnings;
  std::expected<T, std::string> outcome;

  template <typename... Args>
  void set_error(std::format_string<Args...> fmt, Args&&... args) {
    outcome = std::unexpected(std::format(fmt, std::forward<Args>(args)...));
  }

  template <typename... Args>
  void add_warning(std::format_string<Args...> fmt, Args&&... args) {
    warnings[std::format(fmt, std::forward<Args>(args)...)]++;
  }
  // merges warnings from another return code. If the keys match, the
  // numbers are incremented.
  void merge_warnings(const result& other) {
    for (const auto& [msg, count] : other.warnings) {
      warnings[msg] += count;
    }
  }
};

}  // namespace metalldata