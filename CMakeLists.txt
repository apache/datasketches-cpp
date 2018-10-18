cmake_minimum_required(VERSION 3.0)
set(PROJECT_NAME sketches-core-cpp)
set(RELEASE_DATE 2018-10-18)
project(${PROJECT_NAME})
#include(CPPUnit)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_COMPILE_FLAGS "-c -g -Wall -O0")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/tools/cmake")
find_package(cppunit REQUIRED)

include_directories(
    include/hll
    include/kll
)
add_subdirectory(include)
add_subdirectory(src)
add_subdirectory(test)
