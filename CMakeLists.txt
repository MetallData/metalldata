cmake_minimum_required(VERSION 3.14)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(MetallData
        VERSION 0.3
        DESCRIPTION "Metall Containers"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


#
# FetchContent dependencies that can be overridden on the command line.
#
set(BOOST_FETCH_URL
    "https://github.com/boostorg/boost/releases/download/boost-1.87.0/boost-1.87.0-cmake.tar.gz"
    CACHE STRING "URL to fetch Boost tarball")
set(YGM_FETCH_GIT 
    "https://github.com/llnl/ygm.git" 
    CACHE STRING "GIT to fetch YGM")
set(FAKER_FETCH_GIT 
    "https://github.com/cieslarmichal/faker-cxx" 
    CACHE STRING "GIT to fetch Faker-CXX")
set(CLIPPY_FETCH_GIT 
    "https://github.com/LLNL/clippy.git" 
    CACHE STRING "GIT to fetch CLIPPy")
set(METALL_FETCH_GIT 
    "https://github.com/LLNL/metall.git" 
    CACHE STRING "GIT to fetch Metall")
set(SPDLOG_FETCH_GIT 
    "https://github.com/gabime/spdlog.git" 
    CACHE STRING "GIT to fetch SPDLOG")

#
# Boost
#
FetchContent_Declare(
    Boost
    URL ${BOOST_FETCH_URL}
    DOWNLOAD_EXTRACT_TIMESTAMP ON
)
set(BOOST_INCLUDE_LIBRARIES json container unordered interprocess property_tree uuid graph program_options static_string)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
# Boost specific PIC settings
set(Boost_USE_STATIC_LIBS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
FetchContent_MakeAvailable(Boost)

#
#  YGM
#
set(YGM_REQUIRE_PARQUET ON)
set(YGM_INSTALL_PARQUET ON)
FetchContent_Declare(YGM
    GIT_REPOSITORY ${YGM_FETCH_GIT}
    GIT_TAG 7b0ccc69ccb944a42fb1b3021b5d03ade3e4e2b0
        )
FetchContent_MakeAvailable(YGM)

#
#  Faker
#
set(BUILD_TESTING OFF)
FetchContent_Declare(faker
    GIT_REPOSITORY ${FAKER_FETCH_GIT}
    GIT_TAG v4.0.1
    GIT_SUBMODULES ""
        )
FetchContent_MakeAvailable(faker)


#
#  Clippy (which includes jsonlogic)
#
set(JSONLOGIC_ENABLE_BENCH OFF)
set(JSONLOGIC_ENABLE_TESTS OFF)
FetchContent_Declare(CLIPPy
        GIT_REPOSITORY ${CLIPPY_FETCH_GIT}
        GIT_TAG v0.5.1
        SOURCE_SUBDIR cpp
        )
        FetchContent_MakeAvailable(CLIPPy)
set(CLIPPy_INCLUDE_DIR "${clippy_SOURCE_DIR}/cpp/include")
set(jsonlogic_INCLUDE_DIR ${jsonlogic_SOURCE_DIR}/include)
message(STATUS "jsonlogic source dir: ${jsonlogic_SOURCE_DIR}")
message(STATUS "jsonlogic include dir: ${jsonlogic_INCLUDE_DIR}")


#
#  Metall
#
set(JUST_INSTALL_METALL_HEADER TRUE)
FetchContent_Declare(Metall
        GIT_REPOSITORY ${METALL_FETCH_GIT}
        GIT_TAG v0.32
)
FetchContent_MakeAvailable(Metall)


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
    target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include ${BOOST_INCLUDE_DIRS} ${jsonlogic_INCLUDE_DIR})
    target_link_libraries(${name} PRIVATE Boost::json Boost::container Boost::unordered Boost::interprocess Boost::property_tree Boost::uuid Boost::graph Boost::program_options jsonlogic)
    target_link_libraries(${name} PRIVATE Arrow::arrow_shared)
    target_link_libraries(${name} PRIVATE Parquet::parquet_shared)
    target_compile_definitions(${name} PRIVATE METALLDATA_USE_PARQUET)
endfunction()


#
# spdlog
#
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY ${SPDLOG_FETCH_GIT}
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

add_subdirectory(src)
add_subdirectory(include)

if (METALLDATA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif ()
