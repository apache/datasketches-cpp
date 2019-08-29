add_library(common_test OBJECT "")

target_link_libraries(common_test ${CPPUNIT_LIBRARY})

target_include_directories(common_test
  PUBLIC
    ${CPPUNIT_INCLUDE_DIR}
)

target_sources(common_test
  PRIVATE
    test_runner.cpp
    test_type.hpp
    test_allocator.hpp
    test_allocator.cpp
)
