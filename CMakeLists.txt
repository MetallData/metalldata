cmake_minimum_required(VERSION 3.14)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(MetallData
        VERSION 0.2
        DESCRIPTION "Metall Containers"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)




set(BOOST_URL 
    "https://github.com/boostorg/boost/releases/download/boost-1.87.0/boost-1.87.0-cmake.tar.gz" 
    CACHE STRING "URL to fetch Boost tarball")

#
# Boost
FetchContent_Declare(
    Boost 
    URL ${BOOST_URL}
)
set(BOOST_INCLUDE_LIBRARIES json container unordered interprocess property_tree uuid graph)
FetchContent_MakeAvailable(Boost)


#
#  YGM
set(YGM_REQUIRE_PARQUET ON)
set(YGM_INSTALL_PARQUET ON)
FetchContent_Declare(YGM
    GIT_REPOSITORY https://github.com/llnl/ygm.git
    GIT_TAG v0.8-dev
        )
FetchContent_MakeAvailable(YGM)


#
#  Metall
#
set(JUST_INSTALL_METALL_HEADER TRUE)
set(METALL_GIT_TAG "master")
FetchContent_Declare(Metall
        GIT_REPOSITORY https://github.com/LLNL/metall.git
        GIT_TAG ${METALL_GIT_TAG}
)
FetchContent_MakeAvailable(Metall)



#
#  GoogleTest
#
if (METALLDATA_BUILD_TESTS)
    FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG main
    )
    FetchContent_MakeAvailable(googletest)
endif ()

#
#  CXX Filesystem library
#
include(include_cxx_filesystem_library)
include_cxx_filesystem_library()


#
#  Common functions for all MetallData Executables
#
function(add_common_compile_options name)
    # Common
    #target_compile_options(${name} PRIVATE -Wall -Wextra -pedantic)

    # Debug
    target_compile_options(${name} PRIVATE $<$<CONFIG:Debug>:-O0>)
    target_compile_options(${name} PRIVATE $<$<CONFIG:Debug>:-g3>)
    #    if (Linux)
    #        target_compile_options(${name} PRIVATE $<$<CONFIG:Debug>:-pg>)
    #    endif ()

    # Release
    target_compile_options(${name} PRIVATE $<$<CONFIG:Release>:-Ofast>)
    target_compile_options(${name} PRIVATE $<$<CONFIG:Release>:-DNDEBUG>)

    # Release with debug info
    target_compile_options(${name} PRIVATE $<$<CONFIG:RelWithDebInfo>:-Ofast>)
    target_compile_options(${name} PRIVATE $<$<CONFIG:RelWithDebInfo>:-g3>)
    #    if (Linux)
    #        target_compile_options(${name} PRIVATE $<$<CONFIG:RelWithDebInfo>:-pg>)
    #    endif ()
endfunction()

function(add_metalldata_executable name source)
    add_executable(${name} ${source})
    add_common_compile_options(${name})
    target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include ${BOOST_INCLUDE_DIRS})
    target_link_libraries(${name} PRIVATE Boost::json Boost::container Boost::unordered Boost::interprocess Boost::property_tree Boost::uuid Boost::graph)
    target_link_libraries(${name} PRIVATE Arrow::arrow_shared)
    target_link_libraries(${name} PRIVATE Parquet::parquet_shared)
    target_compile_definitions(${name} PRIVATE METALLDATA_USE_PARQUET)
endfunction()


#
# spdlog
#
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.14.1)
FetchContent_MakeAvailable(spdlog)

#
# Function for setting up a target that uses spdlog
#
function(setup_spdlog_target target)
  target_link_libraries(${target} PRIVATE spdlog::spdlog)
endfunction()

#
# Function for setting up a target that uses YGM
#
function(setup_ygm_target exe_name)
    if (${CMAKE_HOST_SYSTEM_NAME} MATCHES "Linux")
        target_link_libraries(${exe_name} PRIVATE rt)
    endif ()
    target_link_libraries(${exe_name} PRIVATE ygm::ygm)
endfunction()

#
# Function for setting up a target that uses CLIPPy
#
function(setup_clippy_target exe_name)
    target_include_directories(${exe_name} PRIVATE ${Boost_INCLUDE_DIRS})
    target_include_directories(${exe_name} PRIVATE ${CLIPPy_INCLUDE_DIR})
endfunction()

#
# Function for setting up a target that uses Metall
#
function(setup_metall_target exe_name)
    target_include_directories(${exe_name} PRIVATE ${Boost_INCLUDE_DIRS})

    #target_link_libraries(${exe_name} PRIVATE Threads::Threads)

    target_link_libraries(${exe_name} PRIVATE Metall)

    if (FOUND_CXX17_FILESYSTEM_LIB)
        if (REQUIRE_LIB_STDCXX_FS)
            target_link_libraries(${exe_name} PRIVATE stdc++fs)
        endif ()
    elseif ()
        target_compile_definitions(${exe_name} PRIVATE "METALL_DISABLE_CXX17_FILESYSTEM_LIB")
    endif ()

    if (METALLDATA_USE_PARQUET AND Arrow_FOUND AND Parquet_FOUND)
        target_link_libraries(${exe_name} PRIVATE Arrow::arrow_shared)
        target_link_libraries(${exe_name} PRIVATE Parquet::parquet_shared)
        target_compile_definitions(${exe_name} PRIVATE METALLDATA_USE_PARQUET)
    endif ()
    target_compile_definitions(${exe_name} PRIVATE "METALL_DISABLE_CONCURRENCY")
endfunction()

#add_subdirectory(src)
add_subdirectory(examples)

if (METALLDATA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif ()