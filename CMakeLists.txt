cmake_minimum_required(VERSION 3.14)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(MetallData
        VERSION 0.1
        DESCRIPTION "Metall Containers"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

#
#  Threads
#
find_package(Threads REQUIRED)


#
# Boost
find_package(Boost 1.78 QUIET)
if (NOT Boost_FOUND)
    FetchContent_Declare(Boost
            URL https://boostorg.jfrog.io/artifactory/main/release/1.78.0/source/boost_1_78_0.tar.bz2)
    FetchContent_GetProperties(Boost)
    if (NOT Boost_POPULATED)
        FetchContent_Populate(Boost)
    endif ()
    set(BOOST_ROOT ${boost_SOURCE_DIR})
    find_package(Boost 1.78)
endif ()


#
#  cereal
find_package(cereal QUIET)
if (NOT cereal_FOUND)
    set(JUST_INSTALL_CEREAL TRUE)
    FetchContent_Declare(cereal
            GIT_REPOSITORY https://github.com/USCiLab/cereal.git
            GIT_TAG master
            )
    FetchContent_MakeAvailable(cereal)
endif ()


#
#  MPI
find_package(MPI)


#
#  YGM
if (MPI_CXX_FOUND)
    find_package(YGM QUIET)
    if (NOT YGM_FOUND)
        set(JUST_INSTALL_YGM TRUE)
        FetchContent_Declare(YGM
                GIT_REPOSITORY https://github.com/LLNL/ygm.git
                GIT_TAG develop
                )
        FetchContent_MakeAvailable(YGM)
    endif ()
endif ()


#
#  Metall
#
find_package(Metall QUIET)
if (NOT Metall_FOUND)
    set(JUST_INSTALL_METALL_HEADER TRUE)
    FetchContent_Declare(Metall
            GIT_REPOSITORY https://github.com/LLNL/metall.git
            GIT_TAG d2c17bbae0ddf9a11e6d25256923a0c365b3eee2
            )
    FetchContent_MakeAvailable(Metall)
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
    target_compile_options(${name} PRIVATE -Wall -Wextra -pedantic)

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
endfunction()


#
#  Clippy
#
find_package(CLIPPy QUIET)
if (NOT CLIPPy_FOUND)
    FetchContent_Declare(CLIPPy
            GIT_REPOSITORY https://github.com/LLNL/clippy-cpp.git
            GIT_TAG master
            )
    FetchContent_GetProperties(CLIPPy)
    if (NOT clippy_POPULATED)
        FetchContent_Populate(CLIPPy)
    endif ()
    set(CLIPPy_INCLUDE_DIR "${clippy_SOURCE_DIR}/include")
endif ()

#
# Function for setting up a target that uses YGM
#
function(setup_ygm_target exe_name)
    if (NOT MPI_CXX_FOUND)
        message(WARNING "YGM requires MPI; skip building ${exe_name}.")
        return()
    endif ()

    target_link_libraries(${exe_name} PRIVATE MPI::MPI_CXX)
    target_link_libraries(${exe_name} PRIVATE cereal)
    target_link_libraries(${exe_name} PRIVATE Threads::Threads)
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
    target_link_libraries(${exe_name} PRIVATE Threads::Threads)
    target_link_libraries(${exe_name} PRIVATE Metall)

    if (FOUND_CXX17_FILESYSTEM_LIB)
        if (REQUIRE_LIB_STDCXX_FS)
            target_link_libraries(${exe_name} PRIVATE stdc++fs)
        endif ()
    elseif ()
        target_compile_definitions(${exe_name} PRIVATE "METALL_DISABLE_CXX17_FILESYSTEM_LIB")
    endif ()
endfunction()

add_subdirectory(src)
#add_subdirectory(test)
#add_subdirectory(example)
