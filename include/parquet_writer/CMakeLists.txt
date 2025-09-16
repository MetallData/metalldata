# Create a library for the parquet writer
add_library(parquet_writer_lib parquet_writer.cpp)
target_link_libraries(parquet_writer_lib PUBLIC
                Arrow::arrow_shared Parquet::parquet_shared)
setup_metall_target(parquet_writer_lib)
setup_ygm_target(parquet_writer_lib)

# Create the main executable
add_metalldata_executable(parquet_writer_example parquet_writer_example.cpp)
setup_metall_target(parquet_writer_example)
setup_ygm_target(parquet_writer_example)
target_link_libraries(parquet_writer_example PUBLIC
                parquet_writer_lib
                Arrow::arrow_shared Parquet::parquet_shared)
