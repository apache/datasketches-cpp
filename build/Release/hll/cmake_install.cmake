# Install script for directory: /Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll

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
  include("/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/hll/test/cmake_install.cmake")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/DataSketches" TYPE FILE FILES
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/hll.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/AuxHashMap.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CompositeInterpolationXTable.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/hll.private.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllSketchImplFactory.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CouponHashSet.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CouponList.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CubicInterpolation.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HarmonicNumbers.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/Hll4Array.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/Hll6Array.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/Hll8Array.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllArray.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllSketchImpl.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllUtil.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/coupon_iterator.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/RelativeErrorTables.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/AuxHashMap-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CompositeInterpolationXTable-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CouponHashSet-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CouponList-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/CubicInterpolation-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HarmonicNumbers-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/Hll4Array-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/Hll6Array-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/Hll8Array-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllArray-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllSketch-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllSketchImpl-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/HllUnion-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/coupon_iterator-internal.hpp"
    "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/hll/include/RelativeErrorTables-internal.hpp"
    )
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
if(CMAKE_INSTALL_LOCAL_ONLY)
  file(WRITE "/Users/andrea.novellini/Code/datasketches/datasketches-cpp/build/Release/hll/install_local_manifest.txt"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
