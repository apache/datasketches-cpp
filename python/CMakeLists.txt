if(NOT DEFINED PYTHON_VERSION_SUFFIX)
  message("No PYTHON_VERSION_SUFFIX defined.")
  message("If building python wrapper, version defaults to 37 to use python37 libraries.")
  message("Rerun cmake with -DPYTHON_VERSION_SUFFIX=<value> to override.")
  set(PYTHON_VERSION_SUFFIX "37")
endif()

find_package(Python COMPONENTS Development)
find_package(Boost 1.53 COMPONENTS python${PYTHON_VERSION_SUFFIX})

if(NOT(${Boost_FOUND} AND ${Python_Development_FOUND}))
  message("If PythonLibs found but not Boost, check the Python version and set -DPYTHON_VERSION_SUFFIX accordingly.")
  #return()
endif()
message("Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIR}")

add_library(python SHARED EXCLUDE_FROM_ALL)

target_link_libraries(python
  common
  hll
  kll
  cpc
  ${Python_LIBRARIES}
  Boost::python${PYTHON_VERSION_SUFFIX}
)

target_include_directories(python
  PRIVATE
    ${Python_INCLUDE_DIRS}
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
)
