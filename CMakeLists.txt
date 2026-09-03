# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

cmake_minimum_required(VERSION 3.28)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(MetallData
        VERSION 0.2
        DESCRIPTION "Metall Containers"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)

# Prioritizes venvs if cmake is being run inside of one.
set(Python3_FIND_VIRTUALENV FIRST)

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#
# Configuration options
#
option(METALLDATA_FIND_PARQUET_IN_SYSTEM "Find Arrow Parquet installed in the system." ON)
option(METALLDATA_INSTALL_PARQUET "Installs Arrow Parquet. If METALLDATA_FIND_PARQUET_IN_SYSTEM is also ON, this option is executed only if Arrow Parquet is not found in the system." ON)
set(METALLDATA_PYTHON_VENV_ROOT CACHE PATH "Path to a Python virtual environment root. If set, MetallData will use this virtual environment for Python dependencies.")
option(BUILD_ONLY_LIBMETALLDATA "Build only the MetallData library." OFF)

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
    "https://github.com/karimyoussef91/metall-1.git"
    CACHE STRING "GIT to fetch Metall")
set(METALL_FETCH_TAG
    "feature/privateer_deps"
    CACHE STRING "Git tag, branch, or commit to fetch for Metall")
set(PRIVATEER_FETCH_GIT
    "https://github.com/LLNL/privateer.git"
    CACHE STRING "GIT to fetch Privateer")
set(PRIVATEER_FETCH_TAG
    "feature/auto_deps"
    CACHE STRING "Git tag, branch, or commit to fetch for Privateer")
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
# Arrow Parquet
#
include(FindArrowParquet)
if (METALLDATA_FIND_PARQUET_IN_SYSTEM)
    find_arrow_parquet_config()
endif ()

# Install Arrow Parquet
if (METALLDATA_INSTALL_PARQUET AND (NOT Arrow_FOUND AND NOT Parquet_FOUND))
    install_arrow_parquet()
endif ()

#
#  YGM
#
# Install Parquet on MEtallData's side
set(YGM_FIND_PARQUET_IN_SYSTEM OFF)
set(YGM_INSTALL_PARQUET OFF)
FetchContent_Declare(YGM
    GIT_REPOSITORY ${YGM_FETCH_GIT}
    GIT_TAG 4345ee92565ffe4e2eb2c5cf02bf3825791fb980
        )
FetchContent_MakeAvailable(YGM)

#
#  Faker
#
set(FAKER_BUILD_TESTING OFF)
FetchContent_Declare(faker
    GIT_REPOSITORY ${FAKER_FETCH_GIT}
    GIT_TAG v4.3.2
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
        GIT_TAG v0.5.7
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
set(USE_PRIVATEER ON CACHE BOOL "Enable Privateer support in Metall")
set(FETCH_PRIVATEER_MAIN OFF CACHE BOOL "Let MetallData provide the Privateer source" FORCE)
set(PRIVATEER_BUILD_TESTS OFF CACHE BOOL "Disable Privateer tests when fetched by MetallData" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "Disable dependency test suites when fetched by MetallData" FORCE)
set(ENABLE_PRIVATEER_TEST OFF CACHE BOOL "Disable Privateer tests when fetched by MetallData" FORCE)
if (USE_PRIVATEER)
    set(FETCH_PRIVATEER_MAIN OFF CACHE BOOL "Let MetallData provide the Privateer source" FORCE)
    FetchContent_Declare(Privateer
        GIT_REPOSITORY ${PRIVATEER_FETCH_GIT}
        GIT_TAG ${PRIVATEER_FETCH_TAG}
    )
    FetchContent_MakeAvailable(Privateer)
    if (TARGET privateer)
        get_target_property(_privateer_compile_definitions privateer INTERFACE_COMPILE_DEFINITIONS)
        if (_privateer_compile_definitions)
            list(REMOVE_ITEM _privateer_compile_definitions SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
            set_target_properties(privateer PROPERTIES INTERFACE_COMPILE_DEFINITIONS "${_privateer_compile_definitions}")
        endif ()
        target_compile_definitions(privateer PUBLIC SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_OFF)
    endif ()
    set(PRIVATEER_ROOT ${privateer_SOURCE_DIR} CACHE PATH "Path to the fetched Privateer source" FORCE)
else ()
    set(FETCH_PRIVATEER_MAIN OFF CACHE BOOL "Disable Metall internal Privateer fetch" FORCE)
    unset(PRIVATEER_ROOT CACHE)
endif ()
FetchContent_Declare(Metall
        GIT_REPOSITORY ${METALL_FETCH_GIT}
    GIT_TAG ${METALL_FETCH_TAG}
)
FetchContent_MakeAvailable(Metall)


#
#  Common functions for all MetallData Executables
#
function(add_common_compile_options name)
    # Common
    # TODO:   Enable after fixing Clippy Issue #46
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
    link_arrow_parquet(${name} PRIVATE)
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
    if (USE_PRIVATEER AND TARGET privateer)
        target_compile_definitions(${exe_name} PRIVATE METALL_USE_PRIVATEER)
        target_include_directories(${exe_name} PRIVATE ${privateer_SOURCE_DIR}/include)
        target_link_libraries(${exe_name} PRIVATE privateer)
    endif ()

    if (FOUND_CXX17_FILESYSTEM_LIB)
        if (REQUIRE_LIB_STDCXX_FS)
            target_link_libraries(${exe_name} PRIVATE stdc++fs)
        endif ()
    elseif ()
        target_compile_definitions(${exe_name} PRIVATE "METALL_DISABLE_CXX17_FILESYSTEM_LIB")
    endif ()

    target_compile_definitions(${exe_name} PRIVATE "METALL_DISABLE_CONCURRENCY")
endfunction()

add_subdirectory(src)

set(ENV_METALLDATA_BUILD_TESTS "$ENV{METALLDATA_BUILD_TESTS}")
if (METALLDATA_BUILD_TESTS OR ENV_METALLDATA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif ()
