add_library(kll INTERFACE)

add_subdirectory(test)

target_include_directories(kll
  INTERFACE
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

target_sources(kll
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_helper.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_quantile_calculator.hpp
)
