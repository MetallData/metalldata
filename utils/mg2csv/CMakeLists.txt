cmake_minimum_required(VERSION 3.20)

# mg2csv executable
add_executable(mg2csv mg2csv.cpp)

target_link_libraries(mg2csv
  PRIVATE
    MetallGraphLib
)

target_include_directories(mg2csv
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src/MetallGraph
    ${CMAKE_SOURCE_DIR}/include
)

# Apply common setup functions
add_common_compile_options(mg2csv)
setup_ygm_target(mg2csv)
setup_metall_target(mg2csv)

# Install target
install(TARGETS mg2csv
  RUNTIME DESTINATION bin
)
