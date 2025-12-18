// Copyright 2024 Lawrence Livermore National Security, LLC and other Metall
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <cstdlib>
#include <filesystem>
#include <vector>

std::vector<std::filesystem::path>
find_files(const std::filesystem::path &path) {
  std::vector<std::filesystem::path> files;

  // If dir is a file, return it as a single element vector
  if (std::filesystem::is_regular_file(path)) {
    files.push_back(path);
    return files;
  }

  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    if (entry.is_regular_file()) {
      files.push_back(entry.path());
    }
  }

  return files;
}

std::string run_command(const std::string &cmd) {
  // std::cout << cmd << std::endl;

  const std::string tmp_file("/tmp/tmp_command_result");
  std::string command(cmd + " > " + tmp_file);
  const auto ret = std::system(command.c_str());
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

std::string get_dir_usage(const std::string &dir_path) {
  return run_command("du -d 0 -h " + dir_path + " | head -n 1");
}