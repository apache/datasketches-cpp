# TODO: Can we force python version >= 3.0?
if (MSVC)
  set(PYBIND11_CPP_STANDARD /std:c++11)
else()
  set(PYBIND11_CPP_STANDARD -std=c++11)
endif()

add_subdirectory(pybind11)

pybind11_add_module(python MODULE EXCLUDE_FROM_ALL SYSTEM THIN_LTO)

target_link_libraries(python
  PRIVATE
    common
    hll
    kll
    cpc
    fi
    theta
    pybind11::module
)

set_target_properties(python PROPERTIES
  PREFIX ""
  OUTPUT_NAME datasketches
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
    src/theta_wrapper.cpp
)
