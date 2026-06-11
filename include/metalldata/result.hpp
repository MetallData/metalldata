#include <string>
#include <map>
#include <expected>
#include <format>
#include <source_location>

namespace metalldata {

/// Return type for metalldata operations.
///
/// Extends std::expected<T, std::string> with a non-fatal warning log.
/// Warnings are keyed by message and counted so repeated warnings accumulate.
/// The result itself IS the expected value — use it directly as std::expected.
///
/// Callers check success/failure and iterate warnings:
/// @code
///   result<size_t> r = some_operation();
///   if (!r) {
///     std::cerr << r.error() << "\n";
///   } else {
///     use(r.value());
///   }
///   for (auto& [msg, n] : r.warnings()) {
///     std::cerr << msg << " (" << n << " times)\n";
///   }
/// @endcode
///
/// Implementors construct from std::unexpected for errors, or assign the value
/// for success, and use add_warning() for non-fatal diagnostics:
/// @code
///   // error path:
///   return std::unexpected(std::format("series {} not found", name));
///
///   // success path (with optional warnings):
///   result<size_t> r;
///   r.add_warning("series {} ignored", name);
///   r = computed_value;
///   return r;
/// @endcode
///
/// When warnings must be accumulated before the value is known, assign the
/// value separately — operator=(T&&) updates only the expected state and
/// preserves any warnings already added:
/// @code
///   result<size_t> r;
///   for (auto& item : items) {
///     if (item.invalid()) {
///       r.add_warning("invalid item {} ignored", item.name());
///       continue;
///     }
///     process(item);
///   }
///   r = compute_result(items);  // warnings intact
///   return r;
/// @endcode
template <typename T = void>
struct result : std::expected<T, std::string> {
  using Base = std::expected<T, std::string>;
  using Base::Base;

  template <typename TT>
  result& operator=(TT&& value) {
    static_cast<Base&>(*this) = std::forward<TT>(value);
    return *this;
  }

  template <typename... Args>
  void add_warning(std::format_string<Args...> fmt, Args&&... args) {
    m_warnings[std::format(fmt, std::forward<Args>(args)...)]++;
  }

  void add_warning(std::string msg) { m_warnings[std::move(msg)]++; }

  void add_warning(
    const std::source_location& loc = std::source_location::current()) {
    add_warning("{}:{} ({})", loc.file_name(), loc.line(), loc.function_name());
  }

  template <typename... Args>
  void add_warnings(size_t n, std::format_string<Args...> fmt, Args&&... args) {
    m_warnings[std::format(fmt, std::forward<Args>(args)...)] += n;
  }
  void add_warnings(size_t n, std::string msg) {
    m_warnings[std::move(msg)] += n;
  }

  const std::map<std::string, size_t>& warnings() const { return m_warnings; }

  // merges warnings from another return code. If the keys match, the
  // numbers are incremented.
  void merge_warnings(const result& other) {
    for (const auto& [msg, count] : other.m_warnings) {
      m_warnings[msg] += count;
    }
  }

 private:
  std::map<std::string, size_t> m_warnings;
};

}  // namespace metalldata