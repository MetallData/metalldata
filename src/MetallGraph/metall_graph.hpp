// Copyright 2021 Lawrence Livermore National Security, LLC and other CLIPPy
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <string_view>
#include <vector>

/* ASSUMPTIONS
Everything is a multigraph
3 multi_series (vertices, directed edges, undirected edges)

internally hardcode u,v has primary col names in edge tables

Edges are not partitioned by u/v hashing

Vertex ids are always string,  col name in vertex dataframe is 'id'.  Vertices
are partitioned by has of id






*/

namespace metalldata {
class metall_graph {
 public:
  struct open_only_tag {};
  struct create_only_tag {};
  struct open_or_create_tag {};

  metall_graph(open_only_tag, std::string_view path);
  metall_graph(create_only_tag, std::string_view path, bool overwrite);
  metall_graph(open_or_create_tag, std::string_view path);

  //
  // Ingest from parquet, provide 2 column names to define an edge, provide if
  // directed, provide list of optional metadata fields
  void ingest_parquet_edges(std::string_view path, std::string_view col_u,
                            std::string_view col_v, bool directed,
                            std::vector<std::string_view> meta);

  void ingest_parquet_verts(std::string_view path, std::string_view key,
                            std::vector<std::string_view> meta,
                            bool                          update = true);

  template <typename T>
  void add_vert_series(std::string_view name);

  template <typename T>
  void add_edge_series(std::string_view name);

  void remove_vert_series(std::string_view name);

  void remove_edge_series(std::string_view name);

  bool contains_vert_series(std::string_view name);

  bool contains_edge_series(std::string_view name);

  ~metall_graph();

 private:
  std::string m_metall_path;

  // metall stuff
  //
};
}  // namespace metalldata