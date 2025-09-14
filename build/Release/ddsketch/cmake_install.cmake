# Install script for directory: /Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch

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
  include("/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/ddsketch/test/cmake_install.cmake")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/DataSketches" TYPE FILE FILES
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/bin.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/bin_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/collapsing_dense_store.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/collapsing_dense_store_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/collapsing_highest_dense_store.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/collapsing_highest_dense_store_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/collapsing_lowest_dense_store.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/collapsing_lowest_dense_store_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/ddsketch.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/ddsketch_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/dense_store.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/dense_store_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/index_mapping.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/index_mapping_factory.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/index_mapping_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/linearly_interpolated_mapping.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/linearly_interpolated_mapping_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/log_like_index_mapping.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/log_like_index_mapping_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/logarithmic_mapping.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/logarithmic_mapping_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/quadratically_interpolated_mapping.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/quadratically_interpolated_mapping_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/quartically_interpolated_mapping.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/quartically_interpolated_mapping_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/sparse_store.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/sparse_store_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/store.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/store_factory.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/unbounded_size_dense_store.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/ddsketch/include/unbounded_size_dense_store_impl.hpp"
    )
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/ddsketch/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
