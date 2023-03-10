#pragma once

#include "MetallJsonLines.hpp"

namespace experimental
{

struct MetallGraph
{
    using edge_list_type = MetallJsonLines;
    using node_list_type = MetallJsonLines;

    template <class OpenTag = metall::open_read_only_t>
    MetallGraph(const MPI_Comm& comm, ygm::comm& world, OpenTag tag, std::string_view loc)
    : edgelst(comm, world, tag, loc.data(), edge_location_suffix),
      nodelst(comm, world, tag, loc.data(), node_location_suffix)
    {}

    edge_list_type&       edges()       { return edgelst; }
    edge_list_type const& edges() const { return edgelst; }

    node_list_type&       nodes()       { return nodelst; }
    node_list_type const& nodes() const { return nodelst; }

    static
    void createOverwrite(const MPI_Comm& comm, ygm::comm& world, std::string_view loc)
    {
      std::string_view edges{edge_location_suffix};
      std::string_view nodes{node_location_suffix};

      MetallJsonLines::createOverwrite2(comm, world, loc, edges);
      MetallJsonLines::createNewOnly2(comm, world, loc, nodes);
    }

    static
    void createNewOnly(const MPI_Comm& comm, ygm::comm& world, std::string_view loc)
    {
      std::string_view edges{edge_location_suffix};
      std::string_view nodes{node_location_suffix};

      MetallJsonLines::createNewOnly2(comm, world, loc, edges);
      MetallJsonLines::createNewOnly2(comm, world, loc, nodes);
    }

  private:
    edge_list_type edgelst;
    node_list_type nodelst;

    static constexpr const char* const edge_location_suffix = "edges";
    static constexpr const char* const node_location_suffix = "nodes";
};

}
