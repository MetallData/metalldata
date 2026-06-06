#include <string>
#include <map>
#include <expected>
#include <format>

namespace metalldata {

/// Return type for metalldata operations.
///
/// Pairs a std::expected<T, std::string> outcome with a non-fatal warning log.
/// Use fail() to set an error, or assign to outcome directly for success.
/// Check outcome for success/failure and retrieve the value or error through
/// it. Warnings are keyed by message and counted so repeated warnings
/// accumulate; use merge_warnings() to combine results across calls.
///
/// Example:
/// @code
///   result<size_t> r;
///   r.outcome = compute_value();  // or: r.set_error("reason");
///   if (!r.outcome) { handle_error(r.outcome.error()); }
///   for (auto& [msg, n] : r.warnings) { log_warning(msg, n); }
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