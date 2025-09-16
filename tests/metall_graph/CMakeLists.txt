include_directories(${PROJECT_SOURCE_DIR}/src/MetallGraph/)

function(add_metallgraph_test name source)
  add_executable(${name} ${source})
  add_common_compile_options(${name})
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include ${BOOST_INCLUDE_DIRS} ${jsonlogic_INCLUDE_DIR})
  target_link_libraries(${name} PRIVATE Boost::json Boost::container Boost::unordered Boost::interprocess Boost::property_tree Boost::uuid Boost::graph Boost::program_options jsonlogic)
  target_link_libraries(${name} PRIVATE Arrow::arrow_shared)
  target_link_libraries(${name} PRIVATE Parquet::parquet_shared)
  target_link_libraries(${name} PRIVATE MetallGraphLib)
  target_compile_definitions(${name} PRIVATE METALLDATA_USE_PARQUET)

  setup_clippy_target(${name})
  setup_ygm_target(${name})
  setup_metall_target(${name})
  # add this back in later when ready
  #add_test(NAME ${name} COMMAND ${name}) 
endfunction()

add_metallgraph_test(test_metall_graph test_metall_graph.cpp)