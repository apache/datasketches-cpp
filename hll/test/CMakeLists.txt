add_executable(hll_test)

#target_include_directories(hll_test
#  PRIVATE
#    ${HLL_INCLUDE_DIR}
#    ${CPPUNIT_INCLUDE_DIR}
#)

target_link_libraries(hll_test hll common_test)

set_target_properties(hll_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

file(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/" HLL_TEST_BINARY_PATH)
target_compile_definitions(hll_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${HLL_TEST_BINARY_PATH}"
)

target_sources(hll_test
  PRIVATE
    AuxHashMapTest.cpp
    CouponHashSetTest.cpp
    CouponListTest.cpp
    CrossCountingTest.cpp
    HllArrayTest.cpp
    HllSketchTest.cpp
    HllUnionTest.cpp
    TablesTest.cpp
    ToFromByteArrayTest.cpp
    UnionCaseTest.cpp
)
