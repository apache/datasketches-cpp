add_library(kll INTERFACE)
add_executable(kll_test)

target_include_directories(kll INTERFACE include)

target_include_directories(kll_test
  PRIVATE
    include
    ${CPPUNIT_INCLUDE_DIR}
)

set(KLL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include PARENT_SCOPE)

target_link_libraries(kll_test kll common_test)

set_target_properties(kll_test PROPERTIES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

file(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/test/" KLL_TEST_BINARY_PATH)
target_compile_definitions(kll_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${KLL_TEST_BINARY_PATH}"
)

target_sources(kll
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_helper.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_quantile_calculator.hpp
)

target_sources_local(kll_test
  PRIVATE
    test/kll_sketch_test.cpp
    test/kll_sketch_validation.cpp
)