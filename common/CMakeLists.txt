add_library(common INTERFACE)

add_subdirectory(test)

target_include_directories(common
  INTERFACE
    include
)

target_sources(common
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/MurmurHash3.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CommonUtil.hpp
)

