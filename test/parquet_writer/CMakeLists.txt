# Add test executable
add_metalldata_executable(test_parquet_writer test_parquet_writer.cpp)
setup_metall_target(test_parquet_writer)
setup_ygm_target(test_parquet_writer)

# Add include directory so the test can find the header
target_include_directories(test_parquet_writer PRIVATE 
    ${CMAKE_SOURCE_DIR}/include/parquet_writer)

target_link_libraries(test_parquet_writer PUBLIC
                parquet_writer_lib
                Arrow::arrow_shared Parquet::parquet_shared)

# Main comprehensive test
add_test(NAME ParquetWriter_Tests COMMAND test_parquet_writer)

# Set test properties
set_tests_properties(ParquetWriter_Tests PROPERTIES
    TIMEOUT 30
    LABELS "parquet;writer;unit;comprehensive;exceptions"
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)