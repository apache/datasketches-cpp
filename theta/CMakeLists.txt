add_library(theta INTERFACE)

add_library(${PROJECT_NAME}::THETA ALIAS theta)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(theta
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_link_libraries(theta INTERFACE common)
target_compile_features(theta INTERFACE cxx_std_11)

set(theta_HEADERS "")
list(APPEND theta_HEADERS "include/theta_sketch.hpp;include/theta_union.hpp;include/theta_intersection.hpp")
list(APPEND theta_HEADERS "include/theta_a_not_b.hpp;include/binomial_bounds.hpp;include/theta_sketch_impl.hpp")
list(APPEND theta_HEADERS "include/theta_union_impl.hpp;include/theta_intersection_impl.hpp;include/theta_a_not_b_impl.hpp")

install(TARGETS theta
  EXPORT ${PROJECT_NAME}
)

install(FILES ${theta_HEADERS}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")

target_sources(theta
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_a_not_b.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/binomial_bounds.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_sketch_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_a_not_b_impl.hpp
)
