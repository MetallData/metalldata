#
# FetchContent dependencies that can be overridden on the command line.
#
set(GTEST_FETCH_GIT 
    "https://github.com/google/googletest.git" 
    CACHE STRING "GIT to fetch Google Test")

FetchContent_Declare(
    googletest
    GIT_REPOSITORY ${GTEST_FETCH_GIT}
    GIT_TAG        v1.17.0 
    GIT_SUBMODULES ""
)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

function(add_gtest_executable name source)
    add_metalldata_executable(${name} ${source})
    setup_metall_target(${name})
    target_link_libraries(${name} PRIVATE GTest::gtest_main)
    gtest_discover_tests(${name})
endfunction()

message(${CMAKE_BINARY_DIR}/src/clippy)

#
# Adds a target for pytest
find_package(Python3 REQUIRED COMPONENTS Interpreter)
set(MPIRUN "mpirun --oversubscribe -np 4")
add_custom_target(pytest
        COMMAND ${CMAKE_COMMAND} -E env
            CLIPPY_BACKEND_PATH=${CMAKE_BINARY_DIR}/src/clippy
            CLIPPY_CMD_PREFIX=${MPIRUN}
            ${Python3_EXECUTABLE} -m pytest tests/
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/clippy
    USES_TERMINAL
)


add_subdirectory(string_table)
add_subdirectory(multiseries)
add_subdirectory(metall_graph)