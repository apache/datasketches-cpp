add_library(kll INTERFACE)

add_library(${PROJECT_NAME}::KLL ALIAS kll)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(kll
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_link_libraries(kll INTERFACE common)
target_compile_features(kll INTERFACE cxx_std_11)

set(kll_HEADERS "include/kll_sketch.hpp;include/kll_helper.hpp;include/kll_quantile_calculator.hpp")

install(TARGETS kll
  EXPORT ${PROJECT_NAME}
)

install(FILES ${kll_HEADERS}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")

target_sources(kll
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_helper.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/kll_quantile_calculator.hpp
)
