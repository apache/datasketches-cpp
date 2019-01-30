if(NOT DEFINED PYTHON_VERSION_SUFFIX)
  message("No PYTHON_VERSION_SUFFIX defined.")
  message("If building python wrapper, version defaults to 37 to use python37 libraries.")
  message("Rerun cmake with -DPYTHON_VERSION_SUFFIX=<value> to override.")
  set(PYTHON_VERSION_SUFFIX "37")
endif()

find_package(PythonLibs 3.0)
find_package(Boost 1.56 COMPONENTS python${PYTHON_VERSION_SUFFIX})

if(NOT(${Boost_FOUND} AND ${PythonLibs_FOUND}))
  message("If PythonLibs found but not Boost, check the Python version and set -DPYTHON_VERSION_SUFFIX accordingly.")
  return()
endif()

add_library(datasketches SHARED EXCLUDE_FROM_ALL)

# ensure we make a .so on Mac rather than .dylib
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set_target_properties(datasketches PROPERTIES SUFFIX ".so")
endif()
# create a more intuitive python target
add_custom_target(python)
add_dependencies(python datasketches)



target_link_libraries(datasketches
  common
  hll
  kll
  cpc
  ${PYTHON_LIBRARIES}
  ${Boost_LIBRARIES}
)

target_include_directories(datasketches
  PRIVATE
    ${HLL_INCLUDE_DIR}
    ${KLL_INCLUDE_DIR}
    ${CPC_INCLUDE_DIR}
    ${COMMON_INCLUDE_DIR}
    ${PYTHON_INCLUDE_DIRS}
    ${Boost_INCLUDE_DIRS}
)

set_target_properties(datasketches PROPERTIES
  PREFIX ""  # python doesn't allow libdatasketches
  LINKER_LANGUAGE CXX
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO   # on Mac, uses -std=c++11 rather than -std=gnu++11
)

target_sources_local(datasketches
  PRIVATE
    src/datasketches.cpp
    src/hll_wrapper.cpp
    src/kll_wrapper.cpp
    src/cpc_wrapper.cpp
)
