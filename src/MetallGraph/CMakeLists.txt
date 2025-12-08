# Copyright 2020 Lawrence Livermore National Security, LLC and other CLIPPy
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

#set(CMAKE_CXX_FLAGS_DEBUG "-Wall -Wextra -pedantic -Wno-unused-parameter -O0 -g")
#set(CMAKE_CXX_FLAGS_RELEASE "-Wall -Wextra -O3 -march=native")

# include_directories(${Boost_INCLUDE_DIRS})

include_directories(${PROJECT_SOURCE_DIR}/src/MetallGraph/)


add_library(MetallGraphLib STATIC metall_graph.cpp metall_graph_assign.cpp metall_graph_where.cpp) 
setup_metall_target(MetallGraphLib)
setup_ygm_target(MetallGraphLib)
#these could be made public if we used the PIMPL technique
target_link_libraries(MetallGraphLib PUBLIC Boost::json Boost::container Boost::unordered Boost::interprocess Boost::property_tree Boost::uuid Boost::graph Boost::program_options jsonlogic)
target_include_directories(MetallGraphLib PUBLIC ${PROJECT_SOURCE_DIR}/include ${BOOST_INCLUDE_DIRS} ${jsonlogic_INCLUDE_DIR})
target_link_libraries(MetallGraphLib PUBLIC Arrow::arrow_shared)
target_link_libraries(MetallGraphLib PUBLIC Parquet::parquet_shared)

function(add_metallgraph_executable name source)
  add_executable(${name} ${source})
  add_common_compile_options(${name})
  target_link_libraries(${name} PRIVATE MetallGraphLib)
  target_compile_definitions(${name} PRIVATE METALLDATA_USE_PARQUET)
  setup_clippy_target(${name})
  setup_ygm_target(${name})
  setup_metall_target(${name})
endfunction()



add_metallgraph_executable(__init__ __init__.cpp)
add_metallgraph_executable(ingest_parquet_edges ingest_parquet_edges.cpp)
add_metallgraph_executable(describe describe.cpp)
add_metallgraph_executable(debug debug.cpp)
add_metallgraph_executable(drop_series drop_series.cpp)
add_metallgraph_executable(assign assign.cpp)
# add_metallgraph_executable(drop_series drop_series.cpp)

add_custom_command(
        TARGET __init__ POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/meta.json
                ${CMAKE_CURRENT_BINARY_DIR}/meta.json)