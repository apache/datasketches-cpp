add_subdirectory(test)

add_library(common INTERFACE)

target_include_directories(common
  INTERFACE
    include
)

set(COMMON_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include PARENT_SCOPE)

target_sources(common
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/MurmurHash3.h
)

