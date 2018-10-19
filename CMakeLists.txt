cmake_minimum_required(VERSION 2.8)
set(PROJECT_NAME sketches-core-cpp)
set(RELEASE_DATE 2018-10-18)
project(${PROJECT_NAME})

add_compile_options(-c -g -Wall -O0 -std=c++11)
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
