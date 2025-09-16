// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include "metall_graph.hpp"
#include <iostream>
#include <string>
#include <string_view>

namespace metalldata {

metall_graph::metall_graph(open_only_tag, std::string_view path)
    : m_metall_path(path) {
  std::cout << "metall_graph() open_only" << std::endl;
}
metall_graph::metall_graph(create_only_tag, std::string_view path,
                           bool overwrite)
    : m_metall_path(path) {
  std::cout << "metall_graph() create_only" << std::endl;
}
metall_graph::metall_graph(open_or_create_tag, std::string_view path)
    : m_metall_path(path) {
  std::cout << "metall_graph() open_or_create" << std::endl;
}

metall_graph::~metall_graph() {}

}  // namespace metalldata