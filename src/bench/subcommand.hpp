// Copyright 2025 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

#pragma once

#include <ygm/comm.hpp>
#include <boost/program_options.hpp>
#include <memory>
#include <map>
#include <stdexcept>
#include <stdexcept>
#include <iostream>

class base_subcommand {
 public:
  virtual std::string name() = 0;
  virtual std::string desc() = 0;

  virtual boost::program_options::options_description get_options() = 0;

  virtual std::string parse(const boost::program_options::variables_map&) = 0;

  virtual int run(ygm::comm&) = 0;
};

class cli_subcommand {
 public:
  cli_subcommand(ygm::comm& comm) : m_comm(comm) {}

  template <typename SubCommand, typename... Args>
  void add_subcommand(Args&&... args) {
    static_assert(std::is_base_of<base_subcommand, SubCommand>::value,
                  "base_subcommand is not a base of SubCommand!");
    std::shared_ptr<SubCommand> sp =
        std::make_shared<SubCommand>(std::forward<Args>(args)...);
    std::string name = sp->name();
    if (m_subcommands.contains(name)) {
      throw std::runtime_error("Subcommand already exists");
    }
    m_subcommands.insert(std::make_pair(std::move(name), sp));
  }

  int run(int argc, char** argv) {
    namespace po = boost::program_options;
    if (argc < 2) {
      priv_print_help(argv[0]);
      return 0;
    }
    std::string selected(argv[1]);
    if (selected == "help") {
      priv_print_help(argv[0]);
      return 0;
    }
    if (m_subcommands.contains(selected)) {
      std::vector<std::string> args_without_cmd;
      args_without_cmd.push_back(argv[0]);
      for (size_t i = 2; i < argc; ++i) {
        args_without_cmd.push_back(argv[i]);
      }
      po::variables_map vm;
      auto              pd = m_subcommands[selected]->get_options();
      try {
        po::store(po::command_line_parser(args_without_cmd).options(pd).run(),
                  vm);
        po::notify(vm);
      } catch (const std::exception& ex) {
        std::cerr << "Error parsing options: " << ex.what() << "\n";
        return 0;
      }
      std::string errmsg = m_subcommands[selected]->parse(vm);
      if (not errmsg.empty()) {
        m_comm.cout0(errmsg);
        m_comm.cout0("Try '", argv[0], " help' for more information.");
        return 0;
      }
      m_comm.barrier();
      return m_subcommands[selected]->run(m_comm);
    } else {
      m_comm.cout0("Unkown subcommand: ", selected);
      m_comm.cout0("Try '", argv[0], " help' for more information.");
      return 0;
    }
  }

 private:
  void priv_print_help(const std::string program_name) {
    m_comm.cout0("Usage: ", program_name, " <command> [options]\n");
    m_comm.cout0("Available commands:\n");
    for (const auto& p : m_subcommands) {
      m_comm.cout0(p.first, "\t", p.second->desc(), "\n");
      m_comm.cout0(p.second->get_options());
    }
  }

  std::map<std::string, std::shared_ptr<base_subcommand>> m_subcommands;
  ygm::comm&                                              m_comm;
};