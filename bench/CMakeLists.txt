add_metalldata_executable(mframe_bench mframe_bench.cpp)
setup_metall_target(mframe_bench)
setup_ygm_target(mframe_bench)
#link_arrow_parquet(mframe_bench)
target_link_libraries(mframe_bench PUBLIC
                Arrow::arrow_shared Parquet::parquet_shared)