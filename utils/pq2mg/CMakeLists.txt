cmake_minimum_required(VERSION 3.20)

# pq2mg executable
add_executable(pq2mg pq2mg.cpp)

target_link_libraries(pq2mg
  PRIVATE
    MetallGraphLib
)

target_include_directories(pq2mg
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src/MetallGraph
    ${CMAKE_SOURCE_DIR}/include
)

# Apply common setup functions
add_common_compile_options(pq2mg)
setup_ygm_target(pq2mg)
setup_metall_target(pq2mg)
target_compile_definitions(pq2mg PRIVATE METALLDATA_USE_PARQUET)

# Install target
install(TARGETS pq2mg
  RUNTIME DESTINATION bin
)
