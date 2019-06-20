add_library(fi INTERFACE)

add_library(${PROJECT_NAME}::FI ALIAS fi)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(fi
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_link_libraries(fi INTERFACE common)
target_compile_features(fi INTERFACE cxx_std_11)

set(fi_HEADERS "include/frequent_items_sketch.hpp;include/reverse_purge_hash_map.hpp")

install(TARGETS fi
  EXPORT ${PROJECT_NAME}
)

install(FILES ${fi_HEADERS}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")

target_sources(fi
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/frequent_items_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/reverse_purge_hash_map.hpp
)
