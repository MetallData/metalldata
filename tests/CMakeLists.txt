include(GoogleTest)

function(add_gtest_executable name source)
    add_metalldata_executable(${name} ${source})
    setup_metall_target(${name})
    target_link_libraries(${name} PRIVATE GTest::gtest_main)
    gtest_discover_tests(${name})
endfunction()

#add_subdirectory(json_bento)
add_subdirectory(string_table)
add_subdirectory(multiseries)
add_subdirectory(parquet_writer)