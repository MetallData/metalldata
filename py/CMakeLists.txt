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
