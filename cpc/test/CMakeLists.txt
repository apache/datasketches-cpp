add_executable(cpc_test)

# target_include_directories(cpc_test
#   PRIVATE 
#     ${COMMON_INCLUDE_DIR}
#     ${CPPUNIT_INCLUDE_DIR}
# )

target_link_libraries(cpc_test cpc common_test)

set_target_properties(cpc_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

#file(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/test/" CPC_TEST_BINARY_PATH)
#target_compile_definitions(cpc_test
#  PRIVATE
#    TEST_BINARY_INPUT_PATH="${CPC_TEST_BINARY_PATH}"
#)

add_test(
  NAME cpc_test
  COMMAND cpc_test
)

target_sources(cpc_test
  PRIVATE
    cpc_sketch_test.cpp
    cpc_union_test.cpp
    compression_test.cpp
)