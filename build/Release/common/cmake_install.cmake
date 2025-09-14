# Install script for directory: /Users/andrea.novellini/Code/datasketches/datasketches-cpp/common

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
  include("/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/common/test/cmake_install.cmake")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/DataSketches" TYPE FILE FILES
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/common/include/version.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/binomial_bounds.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/bounds_binomial_proportions.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/ceiling_power_of_2.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/common_defs.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/conditional_back_inserter.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/conditional_forward.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/count_zeros.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/fast_log2.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/inv_pow2_table.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/kolmogorov_smirnov_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/kolmogorov_smirnov.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/memory_operations.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/MurmurHash3.h"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/optional.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/quantiles_sorted_view_impl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/quantiles_sorted_view.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/serde.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/common/include/xxhash64.h"
    )
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/common/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
