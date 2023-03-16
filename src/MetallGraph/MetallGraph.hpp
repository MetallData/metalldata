#pragma once

#include "MetallJsonLines.hpp"

namespace experimental
{

struct MetallGraph
{
    using edge_list_type      = MetallJsonLines;
    using node_list_type      = MetallJsonLines;
    using metall_manager_type = MetallJsonLines::metall_manager_type;

    MetallGraph(metall_manager_type& manager, ygm::comm& comm)
    : edgelst(manager, comm, edge_location_suffix),
      nodelst(manager, comm, node_location_suffix)
    {}

    edge_list_type&       edges()       { return edgelst; }
    edge_list_type const& edges() const { return edgelst; }

    node_list_type&       nodes()       { return nodelst; }
    node_list_type const& nodes() const { return nodelst; }

    static
    void createNew(metall_manager_type& manager, ygm::comm& comm)
    {
      MetallJsonLines::createNew(manager, comm, {edge_location_suffix, node_location_suffix});
    }

    static
    void checkState(ygm::comm& comm, std::string_view loc)
    {
      MetallJsonLines::checkState(comm, loc, {edge_location_suffix, node_location_suffix});
    }

  private:
    edge_list_type edgelst;
    node_list_type nodelst;

    static constexpr const char* const edge_location_suffix = "edges";
    static constexpr const char* const node_location_suffix = "nodes";
};

}
