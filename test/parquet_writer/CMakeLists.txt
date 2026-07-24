# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

# Add test executable
add_metalldata_executable(test_parquet_writer test_parquet_writer.cpp)
setup_metall_target(test_parquet_writer)
setup_ygm_target(test_parquet_writer)

# Add include directory so the test can find the header
target_include_directories(test_parquet_writer PRIVATE
    ${CMAKE_SOURCE_DIR}/include/parquet_writer)

link_arrow_parquet(test_parquet_writer PUBLIC)

# Main comprehensive test
add_test(NAME ParquetWriter_Tests COMMAND test_parquet_writer)

# Set test properties
set_tests_properties(ParquetWriter_Tests PROPERTIES
    TIMEOUT 30
    LABELS "parquet;writer;unit;comprehensive;exceptions"
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)