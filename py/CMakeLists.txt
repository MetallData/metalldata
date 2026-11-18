# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

#
# Assembles the `metalldata` Python package in ${CMAKE_CURRENT_BINARY_DIR}.
# Use it from the build tree with PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}.
#
# The clippy Python frontend is vendored from the same clippy checkout used
# for the C++ backend (see CLIPPy in the top-level CMakeLists.txt), so the
# two always match.
#
set(METALLDATA_PY_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/metalldata)
set(METALLDATA_PY_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/metalldata)
set(CLIPPY_PY_SRC_DIR ${clippy_SOURCE_DIR}/py/src/clippy)

# Directory containing the clippy backend class directories
# (MetallGraph/, MetallUtils/).
set(METALLDATA_BACKEND_DIR ${CMAKE_BINARY_DIR}/src/clippy)

file(GLOB_RECURSE METALLDATA_PY_SOURCES CONFIGURE_DEPENDS
     ${METALLDATA_PY_SRC_DIR}/*.py)
file(GLOB_RECURSE CLIPPY_PY_SOURCES CONFIGURE_DEPENDS
     ${CLIPPY_PY_SRC_DIR}/*.py ${CLIPPY_PY_SRC_DIR}/VERSION)

configure_file(${METALLDATA_PY_SRC_DIR}/_paths.py.in
               ${METALLDATA_PY_OUT_DIR}/_paths.py @ONLY)

set(METALLDATA_PY_STAMP ${CMAKE_CURRENT_BINARY_DIR}/metalldata_py.stamp)
add_custom_command(
  OUTPUT ${METALLDATA_PY_STAMP}
  COMMAND ${CMAKE_COMMAND} -E copy ${METALLDATA_PY_SOURCES} ${METALLDATA_PY_OUT_DIR}
  COMMAND ${CMAKE_COMMAND} -E remove_directory ${METALLDATA_PY_OUT_DIR}/_clippy
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CLIPPY_PY_SRC_DIR} ${METALLDATA_PY_OUT_DIR}/_clippy
  COMMAND ${CMAKE_COMMAND} -E touch ${METALLDATA_PY_STAMP}
  DEPENDS ${METALLDATA_PY_SOURCES} ${CLIPPY_PY_SOURCES}
  COMMENT "Assembling metalldata Python package"
)
add_custom_target(metalldata_py ALL DEPENDS ${METALLDATA_PY_STAMP})

#
# Python virtual environment.
#
# By default the build owns ${CMAKE_BINARY_DIR}/metalldata-venv: it installs
# the requirements below into it and adds a .pth file pointing at this
# package, so activating the venv is all a user needs to do. With tests
# enabled, the test and dev requirements are installed as well.
#
# If METALLDATA_PYTHON_VENV_ROOT is set, that venv belongs to the user and
# is left untouched.
#
# METALLDATA_VENV_PYTHON is exported to the parent scope for the pytest target.
#
option(METALLDATA_SETUP_PY_VENV "Create and populate the build-owned Python venv." ON)

if (METALLDATA_PYTHON_VENV_ROOT)
  set(METALLDATA_VENV_PYTHON ${METALLDATA_PYTHON_VENV_ROOT}/bin/python PARENT_SCOPE)
  message(STATUS "metalldata: using ${METALLDATA_PYTHON_VENV_ROOT} without modifying it. "
                 "Install ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt into it and set "
                 "PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR} to use the metalldata package.")
elseif (METALLDATA_SETUP_PY_VENV)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  get_build_venv_root(METALLDATA_VENV_DIR)
  set(METALLDATA_VENV_PYTHON ${METALLDATA_VENV_DIR}/bin/python PARENT_SCOPE)

  set(METALLDATA_VENV_REQUIREMENTS ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt)
  if (METALLDATA_TESTS_ENABLED)
    list(APPEND METALLDATA_VENV_REQUIREMENTS
         ${PROJECT_SOURCE_DIR}/test/clippy/requirements.txt
         ${PROJECT_SOURCE_DIR}/test/clippy/requirements-dev.txt)
  endif ()

  configure_file(setup_venv.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/setup_venv.cmake @ONLY)

  set(METALLDATA_VENV_STAMP ${CMAKE_CURRENT_BINARY_DIR}/metalldata_venv.stamp)
  add_custom_command(
    OUTPUT ${METALLDATA_VENV_STAMP}
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/setup_venv.cmake
    COMMAND ${CMAKE_COMMAND} -E touch ${METALLDATA_VENV_STAMP}
    DEPENDS ${METALLDATA_VENV_REQUIREMENTS} ${CMAKE_CURRENT_BINARY_DIR}/setup_venv.cmake
    COMMENT "Setting up Python venv ${METALLDATA_VENV_DIR}"
    USES_TERMINAL
  )
  add_custom_target(metalldata_venv ALL DEPENDS ${METALLDATA_VENV_STAMP})
endif ()
