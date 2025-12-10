// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#define WITH_YGM 1
#include <clippy/clippy.hpp>
#include <ygm/comm.hpp>
#include <metalldata/metall_graph.hpp>
#include <faker-cxx/number.h>
#include <faker-cxx/string.h>
#include <faker-cxx/date.h>
#include <faker-cxx/internet.h>
#include <faker-cxx/person.h>
#include <unordered_map>
#include <functional>

static const std::string method_name = "add_faker";
static const std::string state_name  = "INTERNAL";

// Type-erased generator function for metall_graph
using generator_func = std::function<void(
  metalldata::metall_graph&, const metalldata::metall_graph::series_name&,
  const metalldata::metall_graph::where_clause&)>;

// Generator registry
class GeneratorRegistry {
 public:
  void register_generator(const std::string& type_name, generator_func gen) {
    generators_[type_name] = gen;
  }

  generator_func get_generator(const std::string& type_name) const {
    auto it = generators_.find(type_name);
    return (it != generators_.end()) ? it->second : nullptr;
  }

  std::vector<std::string> get_available_types() const {
    std::vector<std::string> types;
    for (const auto& [name, _] : generators_) {
      types.push_back(name);
    }
    return types;
  }

 private:
  std::unordered_map<std::string, generator_func> generators_;
};

// Initialize all generators
inline GeneratorRegistry create_registry() {
  GeneratorRegistry registry;

  registry.register_generator(
    "uuid4", [](metalldata::metall_graph&                     mg,
                const metalldata::metall_graph::series_name&  name,
                const metalldata::metall_graph::where_clause& where) {
      auto gen = []() -> std::string_view {
        static thread_local std::string uuid = faker::string::uuidV4();
        uuid                                 = faker::string::uuidV4();
        return uuid;
      };
      mg.add_faker_series<decltype(gen), std::string_view>(name, gen, where);
    });

  registry.register_generator(
    "integer", [](metalldata::metall_graph&                     mg,
                  const metalldata::metall_graph::series_name&  name,
                  const metalldata::metall_graph::where_clause& where) {
      auto gen = []() { return faker::number::integer<int64_t>(10'000'000); };
      mg.add_faker_series<decltype(gen), int64_t>(name, gen, where);
    });

  registry.register_generator(
    "uint", [](metalldata::metall_graph&                     mg,
               const metalldata::metall_graph::series_name&  name,
               const metalldata::metall_graph::where_clause& where) {
      auto gen = []() { return faker::number::integer<uint64_t>(10'000'000); };
      mg.add_faker_series<decltype(gen), uint64_t>(name, gen, where);
    });

  registry.register_generator(
    "double", [](metalldata::metall_graph&                     mg,
                 const metalldata::metall_graph::series_name&  name,
                 const metalldata::metall_graph::where_clause& where) {
      auto gen = []() { return faker::number::decimal<double>(10'000'000.0); };
      mg.add_faker_series<decltype(gen), double>(name, gen, where);
    });

  registry.register_generator(
    "percentage", [](metalldata::metall_graph&                     mg,
                     const metalldata::metall_graph::series_name&  name,
                     const metalldata::metall_graph::where_clause& where) {
      auto gen = []() { return faker::number::decimal<double>(0.0, 100.0); };
      mg.add_faker_series<decltype(gen), double>(name, gen, where);
    });

  registry.register_generator(
    "int_percentage", [](metalldata::metall_graph&                     mg,
                         const metalldata::metall_graph::series_name&  name,
                         const metalldata::metall_graph::where_clause& where) {
      auto gen = []() { return faker::number::integer<uint64_t>(100); };
      mg.add_faker_series<decltype(gen), uint64_t>(name, gen, where);
    });

  registry.register_generator(
    "two_char_string", [](metalldata::metall_graph&                     mg,
                          const metalldata::metall_graph::series_name&  name,
                          const metalldata::metall_graph::where_clause& where) {
      auto gen = []() -> std::string_view {
        static thread_local std::string s = faker::string::alpha(2);
        s                                 = faker::string::alpha(2);
        return s;
      };
      mg.add_faker_series<decltype(gen), std::string_view>(name, gen, where);
    });

  registry.register_generator(
    "bool", [](metalldata::metall_graph&                     mg,
               const metalldata::metall_graph::series_name&  name,
               const metalldata::metall_graph::where_clause& where) {
      auto gen = []() { return faker::number::integer(0, 1) == 1; };
      mg.add_faker_series<decltype(gen), bool>(name, gen, where);
    });

  registry.register_generator(
    "name", [](metalldata::metall_graph&                     mg,
               const metalldata::metall_graph::series_name&  name,
               const metalldata::metall_graph::where_clause& where) {
      auto gen = []() -> std::string_view {
        static thread_local std::string n = faker::person::fullName();
        n                                 = faker::person::fullName();
        return n;
      };
      mg.add_faker_series<decltype(gen), std::string_view>(name, gen, where);
    });

  registry.register_generator(
    "email", [](metalldata::metall_graph&                     mg,
                const metalldata::metall_graph::series_name&  name,
                const metalldata::metall_graph::where_clause& where) {
      auto gen = []() -> std::string_view {
        static thread_local std::string e = faker::internet::email();
        e                                 = faker::internet::email();
        return e;
      };
      mg.add_faker_series<decltype(gen), std::string_view>(name, gen, where);
    });

  registry.register_generator(
    "username", [](metalldata::metall_graph&                     mg,
                   const metalldata::metall_graph::series_name&  name,
                   const metalldata::metall_graph::where_clause& where) {
      auto gen = []() -> std::string_view {
        static thread_local std::string u = faker::internet::username();
        u                                 = faker::internet::username();
        return u;
      };
      mg.add_faker_series<decltype(gen), std::string_view>(name, gen, where);
    });

  return registry;
}

int main(int argc, char** argv) {
  ygm::comm comm(&argc, &argv);

  clippy::clippy clip{
    method_name,
    "Creates a series and assigns fake values based on a faker function"};
  clip.add_required_state<std::string>("path", "Storage path for MetallGraph");
  clip.add_required<std::string>("series_name", "series name to create");
  clip.add_required<std::string>(
    "generator_type",
    "type of faker generator (uuid4, integer, double, name, email, etc.)");
  clip.add_optional<boost::json::object>("where", "where clause",
                                         boost::json::object{});

  // no object-state requirements in constructor
  if (clip.parse(argc, argv, comm)) {
    return 0;
  }

  auto path           = clip.get_state<std::string>("path");
  auto where          = clip.get<boost::json::object>("where");
  auto name_str       = clip.get<std::string>("series_name");
  auto generator_type = clip.get<std::string>("generator_type");

  metalldata::metall_graph::series_name name(name_str);

  metalldata::metall_graph::where_clause where_c;
  if (where.contains("rule")) {
    where_c = metalldata::metall_graph::where_clause(where["rule"]);
  }

  metalldata::metall_graph mg(comm, path, false);

  // Get the generator registry
  static auto registry = create_registry();

  auto gen = registry.get_generator(generator_type);
  if (!gen) {
    comm.cerr0("Unknown generator type: ", generator_type);
    comm.cerr0("Available types: ");
    for (const auto& type : registry.get_available_types()) {
      comm.cerr0("  - ", type);
    }
    return 1;
  }

  // Call the generator
  gen(mg, name, where_c);

  clip.update_selectors(mg.get_selector_info());
  return 0;
}