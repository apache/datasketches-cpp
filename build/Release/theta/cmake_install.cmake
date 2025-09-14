# Install script for directory: /Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Release")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set path to fallback-tool for dependency-resolution.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for the subdirectory.
  include("/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/theta/test/cmake_install.cmake")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/DataSketches" TYPE FILE FILES
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_sketch.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_sketch_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_union.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_union_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_intersection.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_intersection_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_a_not_b.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_a_not_b_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_jaccard_similarity.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_comparators.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_constants.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_helpers.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_update_sketch_base.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_update_sketch_base_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_union_base.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_union_base_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_intersection_base.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_intersection_base_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_set_difference_base.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_set_difference_base_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/theta_jaccard_similarity_base.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/bounds_on_ratios_in_sampled_sets.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/bounds_on_ratios_in_theta_sketched_sets.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/compact_theta_sketch_parser.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/compact_theta_sketch_parser_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/theta/include/bit_packing.hpp"
    )
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/theta/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
