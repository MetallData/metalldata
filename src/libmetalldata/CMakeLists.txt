# Copyright 2025 Lawrence Livermore National Security, LLC and other CLIPPy
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


#List all future metall_graph cpp files here
add_library(libmetalldata STATIC metall_graph.cpp metall_graph_assign.cpp metall_graph_where.cpp metall_graph_dump_parquet.cpp metall_graph_erase.cpp metall_graph_select.cpp) 
set_target_properties(libmetalldata PROPERTIES
  IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/lib/libmetalldata.so"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/include/metalldata"
)

setup_metall_target(libmetalldata)
setup_ygm_target(libmetalldata)
target_link_libraries(libmetalldata PUBLIC Boost::json Boost::container Boost::unordered Boost::interprocess Boost::property_tree Boost::uuid Boost::graph Boost::program_options jsonlogic parquet_writer_lib faker-cxx)
target_include_directories(libmetalldata PUBLIC ${PROJECT_SOURCE_DIR}/include ${BOOST_INCLUDE_DIRS} ${jsonlogic_INCLUDE_DIR})
target_link_libraries(libmetalldata PUBLIC Arrow::arrow_shared)
target_link_libraries(libmetalldata PUBLIC Parquet::parquet_shared)