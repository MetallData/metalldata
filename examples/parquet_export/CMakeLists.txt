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

# Add test executable
add_metalldata_executable(test_parquet_writer_simple test_parquet_writer_simple.cpp)
setup_metall_target(test_parquet_writer_simple)
setup_ygm_target(test_parquet_writer_simple)
target_link_libraries(test_parquet_writer_simple PUBLIC
                parquet_writer_lib
                Arrow::arrow_shared Parquet::parquet_shared)

# Enable testing and add test
enable_testing()
add_test(NAME ParquetWriter_Tests COMMAND test_parquet_writer_simple)
