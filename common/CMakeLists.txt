add_library(common INTERFACE)
add_library(common_test OBJECT)

target_include_directories(common INTERFACE include)

set(COMMON_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include PARENT_SCOPE)

target_include_directories(common_test PRIVATE ${CPPUNIT_INCLUDE_DIR})
target_link_libraries(common_test ${CPPUNIT_LIBRARY})

target_sources(common
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/MurmurHash3.h
)

target_sources(common_test
  PRIVATE
    test/test_runner.cpp
)
