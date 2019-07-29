add_executable(kll_test)

target_link_libraries(kll_test kll common_test)

set_target_properties(kll_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

file(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/" KLL_TEST_BINARY_PATH)
target_compile_definitions(kll_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${KLL_TEST_BINARY_PATH}"
)

add_test(
  NAME kll_test
  COMMAND kll_test
)

target_sources(kll_test
  PRIVATE
    kll_sketch_test.cpp
    kll_sketch_custom_type_test.cpp
    kll_sketch_validation.cpp
)

target_include_directories(kll_test
  PRIVATE
    ../../common/test
)
