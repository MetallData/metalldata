# Copyright 2025 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()


#List all future metall_graph cpp files here
add_library(libmetalldata STATIC 
            metall_graph.cpp 
            metall_graph_cc.cpp 
            metall_graph_ingest.cpp 
            metall_graph_nhops.cpp 
            metall_graph_degree.cpp 
            metall_graph_assign.cpp 
            metall_graph_where.cpp 
            metall_graph_dump_parquet.cpp 
            metall_graph_erase.cpp 
            metall_graph_select.cpp 
            metall_graph_sample.cpp 
            metall_graph_nunique.cpp 
            metall_graph_value_counts.cpp 
            metall_graph_priv_where_subgraph.cpp
            metall_graph_indexing.cpp
            metall_graph_series.cpp
            metall_graph_locator.cpp) 
set_target_properties(libmetalldata PROPERTIES
  IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/lib/libmetalldata.so"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/include/metalldata"
)

setup_metall_target(libmetalldata)
setup_ygm_target(libmetalldata)
target_link_libraries(libmetalldata PUBLIC 
                      Boost::json 
                      Boost::container 
                      Boost::unordered 
                      Boost::interprocess 
                      Boost::property_tree 
                      Boost::uuid 
                      Boost::graph 
                      jsonlogic 
                      faker-cxx)
target_include_directories(libmetalldata PUBLIC 
                      ${PROJECT_SOURCE_DIR}/include 
                      ${BOOST_INCLUDE_DIRS} 
                      ${jsonlogic_INCLUDE_DIR})
link_arrow_parquet(libmetalldata PUBLIC)