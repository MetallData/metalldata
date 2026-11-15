// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <metalldata/metall_graph.hpp>

namespace metalldata {

/// Undirected template (search pattern) graph for PruneJuice pattern matching.
/// Each template vertex and edge carries a where_clause predicate; an empty
/// clause matches everything. Predicates use the usual where-clause semantics,
/// so a vertex predicate over edge series matches nodes incident to a matching
/// edge, and an edge predicate over node series matches edges whose endpoints
/// both match.
struct metall_graph::pattern_template {
  struct edge {
    size_t       u;
    size_t       v;
    where_clause pred;
  };

  static constexpr size_t max_vertices = 64;
  static constexpr size_t max_edges = 64;

  std::vector<std::string>  vertex_names;
  std::vector<where_clause> vertex_preds;  // parallel to vertex_names
  std::vector<edge>         edges;

  size_t num_vertices() const { return vertex_names.size(); }

  /// Checks: 1..64 vertices, 1..64 edges, valid endpoints, no self loops or
  /// duplicate edges, connected, and every predicate is empty or a pure node
  /// or edge clause.
  result<> validate() const;
};

struct metall_graph::lcc_stats {
  size_t rounds = 0;     ///< number of LCC rounds until fixpoint
  size_t num_nodes = 0;  ///< surviving nodes (global)
  size_t num_edges = 0;  ///< surviving edges (global, counting parallel edges)
};

}  // namespace metalldata
