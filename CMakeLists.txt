cmake_minimum_required(VERSION 3.1.0)
project(datasketches VERSION 0.1.0 LANGUAGES CXX)

# NOTE: This helper function assumes no generator expressions are used
#       for the source files
#       From https://crascit.com/2016/01/31/enhanced-source-file-handling-with-target_sources/
function(target_sources_local target)
  if(POLICY CMP0076)
    # New behavior is available, so just forward to it by ensuring
    # that we have the policy set to request the new behavior, but
    # don't change the policy setting for the calling scope
    cmake_policy(PUSH)
    cmake_policy(SET CMP0076 NEW)
    target_sources(${target} ${ARGN})
    cmake_policy(POP)
    return()
  endif()

  # Must be using CMake 3.12 or earlier, so simulate the new behavior
  unset(_srcList)
  get_target_property(_targetSourceDir ${target} SOURCE_DIR)

  foreach(src ${ARGN})
    if(NOT src STREQUAL "PRIVATE" AND
       NOT src STREQUAL "PUBLIC" AND
       NOT src STREQUAL "INTERFACE" AND
       NOT IS_ABSOLUTE "${src}")
      # Relative path to source, prepend relative to where target was defined
      file(RELATIVE_PATH src "${_targetSourceDir}" "${CMAKE_CURRENT_LIST_DIR}/${src}")
    endif()
    list(APPEND _srcList ${src})
  endforeach()
  target_sources(${target} ${_srcList})
endfunction()

#include(CTest)
#enable_testing()

add_library(datasketches SHARED "")

add_subdirectory(common)
add_subdirectory(hll)
add_subdirectory(cpc)
add_subdirectory(kll)
#add_subdirectory(python)

target_compile_features(datasketches PRIVATE cxx_std_11)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)
