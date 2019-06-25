#find_package(Python3 REQUIRED COMPONENTS Development)

if (MSVC)
  set(PYBIND11_CPP_STANDARD /std:c++11)
else()
  set(PYBIND11_CPP_STANDARD -std=c++11)
endif()

add_subdirectory(pybind11)
message(STATUS "Found pybind11 v${pybind11_VERSION}: ${pybind11_INCLUDE_DIRS}")

pybind11_add_module(python MODULE EXCLUDE_FROM_ALL)
#add_library(python SHARED EXCLUDE_FROM_ALL)

target_link_libraries(python
  PRIVATE
    common
    hll
    #kll
    cpc
    fi
    #${Python3_LIBRARIES}
    pybind11::module
)

#target_include_directories(python
#  PRIVATE
#    ${Python3_INCLUDE_DIRS}
#)

set_target_properties(python PROPERTIES
  PREFIX ""
  OUTPUT_NAME datasketches
#  POSITION_INDEPENDENT_CODE ON
#  LINKER_LANGUAGE CXX
#  CXX_STANDARD 11
#  CXX_STANDARD_REQUIRED YES
)

# ensure we make a .so on Mac rather than .dylib
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set_target_properties(python PROPERTIES SUFFIX ".so")
endif()

target_sources(python
  PRIVATE
    src/datasketches.cpp
    src/hll_wrapper.cpp
    #src/kll_wrapper.cpp
    src/cpc_wrapper.cpp
    src/fi_wrapper.cpp
)
