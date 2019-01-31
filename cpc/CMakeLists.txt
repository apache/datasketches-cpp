add_library(cpc OBJECT "")
add_executable(cpc_test)

target_include_directories(cpc
  PUBLIC
    include
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_include_directories(cpc_test
  PRIVATE 
    include
    ${COMMON_INCLUDE_DIR}
    ${CPPUNIT_INCLUDE_DIR}
)

set(CPC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include PARENT_SCOPE)

target_link_libraries(cpc_test cpc common_test)

set_target_properties(cpc PROPERTIES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

set_target_properties(cpc_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

#file(TO_NATIVE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/test/" CPC_TEST_BINARY_PATH)
#target_compile_definitions(cpc_test
#  PRIVATE
#    TEST_BINARY_INPUT_PATH="${CPC_TEST_BINARY_PATH}"
#)

target_sources_local(cpc
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_union.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_common.hpp
  PRIVATE
    src/compressionData.data
    src/cpc_sketch.cpp
    src/fm85.cpp
    src/fm85Compression.cpp
    src/fm85Confidence.cpp
    src/fm85Merging.cpp
    src/fm85Util.cpp
    src/iconEstimator.cpp
    src/u32Table.cpp
    include/common.h
    include/fm85.h
    include/fm85Compression.h
    include/fm85Confidence.h
    include/fm85Merging.h
    include/fm85Util.h
    include/iconEstimator.h
    include/u32Table.h
)

target_sources_local(cpc_test
  PRIVATE
    test/cpc_sketch_test.cpp
    test/cpc_union_test.cpp
    test/compression_test.cpp
)