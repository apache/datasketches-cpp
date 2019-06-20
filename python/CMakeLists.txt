find_package(Python3 COMPONENTS Development)

if(NOT DEFINED Python3_VERSION_SUFFIX)
  set(Python3_VERSION_SUFFIX "${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}")
  message("Defaulting to look for libboost_python${Python3_VERSION_SUFFIX}")
endif()

find_package(Boost 1.53 COMPONENTS python${Python3_VERSION_SUFFIX} REQUIRED)

if(NOT(${Boost_FOUND} AND ${Python3_Development_FOUND}))
  message("If Python3 found but not Boost, check the Python3 version and set -DPython3_VERSION_SUFFIX to match the suffix on libboost_python.")
  return()
endif()

add_library(python SHARED EXCLUDE_FROM_ALL)

target_link_libraries(python
  common
  hll
  kll
  cpc
  fi
  ${Python3_LIBRARIES}
  Boost::python${Python3_VERSION_SUFFIX}
)

target_include_directories(python
  PRIVATE
    ${Python3_INCLUDE_DIRS}
    ${Boost_INCLUDE_DIRS}
)

set_target_properties(python PROPERTIES
  PREFIX ""
  OUTPUT_NAME datasketches
  POSITION_INDEPENDENT_CODE ON
  LINKER_LANGUAGE CXX
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

# ensure we make a .so on Mac rather than .dylib
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set_target_properties(python PROPERTIES SUFFIX ".so")
endif()

target_sources(python
  PRIVATE
    src/datasketches.cpp
    src/hll_wrapper.cpp
    src/kll_wrapper.cpp
    src/cpc_wrapper.cpp
    src/fi_wrapper.cpp
)
