#pragma once

#include "MetallJsonLines.hpp"

namespace experimental
{

std::string concat(std::string_view lhs, const char* rhs)
{
  std::string res(&*lhs.begin(), lhs.size());

  res += rhs;
  return res;
}

struct MetallGraph
{
    using edge_list_type = MetallJsonLines;
    using node_list_type = MetallJsonLines;

    template <class OpenTag = metall::open_read_only_t>
    MetallGraph(ygm::comm& world, OpenTag tag, std::string_view loc, const MPI_Comm& comm)
    : edgelst(world, tag, concat(loc, edge_location_suffix), comm),
      nodelst(world, tag, concat(loc, node_location_suffix), comm)
    {}

    edge_list_type&       edges()       { return edgelst; }
    edge_list_type const& edges() const { return edgelst; }

    node_list_type&       nodes()       { return nodelst; }
    node_list_type const& nodes() const { return nodelst; }

    static
    void createOverwrite(ygm::comm& world, std::string_view loc, const MPI_Comm& comm)
    {
      MetallJsonLines::createOverwrite(world, concat(loc, edge_location_suffix), comm);
      MetallJsonLines::createOverwrite(world, concat(loc, node_location_suffix), comm);
    }

    static
    void createNewOnly(ygm::comm& world, std::string_view loc, const MPI_Comm& comm)
    {
      MetallJsonLines::createNewOnly(world, concat(loc, edge_location_suffix), comm);
      MetallJsonLines::createNewOnly(world, concat(loc, node_location_suffix), comm);
    }

  private:
    edge_list_type edgelst;
    node_list_type nodelst;

    static constexpr const char* const edge_location_suffix = "/edges";
    static constexpr const char* const node_location_suffix = "/nodes";
};

}
