#add_metalldata_executable(json_bento_simple json_bento_simple.cpp)
#setup_metall_target(json_bento_simple)
#
#add_metalldata_executable(json_bento_bench json_bento_bench.cpp)
#setup_metall_target(json_bento_bench)

add_subdirectory(multiseries)
add_subdirectory(parquet_writer)