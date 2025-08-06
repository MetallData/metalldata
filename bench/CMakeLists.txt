add_metalldata_executable(mframe_bench mframe_bench.cpp)
add_dependencies(mframe_bench jsonlogic)
setup_metall_target(mframe_bench)
setup_ygm_target(mframe_bench)
#link_arrow_parquet(mframe_bench)
target_link_libraries(mframe_bench PUBLIC
                Arrow::arrow_shared Parquet::parquet_shared jsonlogic faker-cxx)


add_metalldata_executable(test_subcommand test_subcommand.cpp)
setup_metall_target(test_subcommand)
setup_ygm_target(test_subcommand)
#link_arrow_parquet(test_subcommand)
target_link_libraries(test_subcommand PUBLIC
                Arrow::arrow_shared Parquet::parquet_shared)