#pragma once
#include <metall/metall.hpp>
#include <multiseries/multiseries_record.hpp>
#include <boost/program_options.hpp>

using record_store_type =
    multiseries::basic_record_store<metall::manager::allocator_type<std::byte>>;
using string_store_type = record_store_type::string_store_type;

using persistent_string =
    boost::container::basic_string<char, std::char_traits<char>,
                                   metall::manager::allocator_type<char>>;

// Generic make_hash function using std::hash
template <typename T>
size_t make_hash(const T& value) {
  std::hash<T> hasher;
  return hasher(value);
}

inline std::string run_command(const std::string& cmd) {
  // std::cout << cmd << std::endl;

  const std::string tmp_file("/tmp/tmp_command_result");
  std::string       command(cmd + " > " + tmp_file);
  const auto        ret = std::system(command.c_str());
  if (ret != 0) {
    return std::string("Failed to execute: " + cmd);
  }

  std::ifstream ifs(tmp_file);
  if (!ifs.is_open()) {
    return std::string("Failed to open: " + tmp_file);
  }

  std::string buf;
  buf.assign((std::istreambuf_iterator<char>(ifs)),
             std::istreambuf_iterator<char>());

  metall::mtlldetail::remove_file(tmp_file);

  return buf;
}

inline std::string get_dir_usage(const std::string& dir_path) {
  return run_command("du -d 0 -h " + dir_path + " | head -n 1");
}