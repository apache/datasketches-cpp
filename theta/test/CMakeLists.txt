add_executable(theta_test)

target_link_libraries(theta_test theta common_test)

set_target_properties(theta_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

file(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/" THETA_TEST_BINARY_PATH)
target_compile_definitions(theta_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${THETA_TEST_BINARY_PATH}"
)

add_test(
  NAME theta_test
  COMMAND theta_test
)

target_sources(theta_test
  PRIVATE
    theta_sketch_test.cpp
    theta_union_test.cpp
    theta_intersection_test.cpp
    theta_a_not_b_test.cpp
)
