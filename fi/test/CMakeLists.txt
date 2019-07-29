add_executable(fi_test)

target_link_libraries(fi_test fi common_test)

set_target_properties(fi_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

file(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/" FI_TEST_BINARY_PATH)
target_compile_definitions(fi_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${FI_TEST_BINARY_PATH}"
)

add_test(
  NAME fi_test
  COMMAND fi_test
)

target_sources(fi_test
  PRIVATE
    reverse_purge_hash_map_test.cpp
    frequent_items_sketch_test.cpp
    frequent_items_sketch_custom_type_test.cpp
)

target_include_directories(fi_test
  PRIVATE
    ../../common/test
)
