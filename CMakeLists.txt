cmake_minimum_required(VERSION 3.1.0)
project(datasketches-core VERSION 0.1.0 LANGUAGES CXX)

### Require out-of-source builds
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
if(EXISTS "${LOC_PATH}")
    message(FATAL_ERROR "You cannot build in a source directory (or any directory with a CMakeLists.txt file). Please make a build subdirectory. Feel free to remove CMakeCache.txt and CMakeFiles.")
endif()

# set default build type to debug
# Mostly from: https://blog.kitware.com/cmake-and-the-default-build-type/
set(default_build_type "Debug")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

# enable compiler warnings globally
# derived from https://foonathan.net/blog/2018/10/17/cmake-warnings.html
# and https://arne-mertz.de/2018/07/cmake-properties-options/
# TODO: make work with generator expressions
if (MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -pedantic)
endif()
# add_compile_options(
#   # Clang/GCC
#   $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:GNU>>:
#     -Wall -pedantic >
#   # Microsoft Visual Studio
#   $<$<CXX_COMPILER_ID:MSVC>:
#     /W4 >
# )

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

find_package(CppUnit REQUIRED)

add_library(datasketches-core SHARED "")

add_subdirectory(common)
add_subdirectory(hll)
add_subdirectory(cpc)
add_subdirectory(kll)
add_subdirectory(python)

target_link_libraries(datasketches-core hll cpc kll)

set_target_properties(datasketches-core PROPERTIES
  LINKER_LANGUAGE CXX
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

# Testing
enable_testing()
add_test(
  NAME hll_test
  COMMAND hll_test
)
add_test(
  NAME kll_test
  COMMAND kll_test
)
add_test(
  NAME cpc_test
  COMMAND cpc_test
)

# # Installation
# include(GNUInstallDirs)
# install(TARGETS datasketches-core
#   #EXPORT datasketches-core-export
#   LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#   INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
#   PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
# )
# # KLL gets special handling as a header-only library until we have exports working
# install(DIRECTORY ${KLL_INCLUDE_DIR}/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

#set(CPACK_PROJECT_NAME ${PROJECT_NAME})
#set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
#include(CPack)
