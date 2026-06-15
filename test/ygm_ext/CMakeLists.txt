

function(add_ygm_ext_test name source)
  add_executable(${name} ${source})
  add_common_compile_options(${name})
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include ${BOOST_INCLUDE_DIRS} ${jsonlogic_INCLUDE_DIR} ${PROJECT_SOURCE_DIR}/src/MetallGraph)
  target_link_libraries(${name} PRIVATE Boost::json Boost::container Boost::unordered Boost::interprocess Boost::property_tree Boost::uuid Boost::graph Boost::program_options jsonlogic)
  target_compile_definitions(${name} PRIVATE CMAKE_DATA_PATH="${CMAKE_SOURCE_DIR}/data"
)

  setup_ygm_target(${name})
  # add this back in later when ready
  add_test(NAME ${name} COMMAND ${name})
endfunction()


add_ygm_ext_test(test_simple_edgelist test_simple_edgelist.cpp)
add_ygm_ext_test(test_static_graph test_static_graph.cpp)