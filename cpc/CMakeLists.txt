add_subdirectory(test)

add_library(cpc OBJECT "")

target_include_directories(cpc
  PUBLIC
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_link_libraries(cpc common)

set(CPC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include PARENT_SCOPE)

set_target_properties(cpc PROPERTIES
  PUBLIC_HEADER "${CPC_INCLUDE_DIR}/cpc_sketch.hpp;${CPC_INCLUDE_DIR}/cpc_union.hpp;${CPC_INCLUDE_DIR}/cpc_common.hpp"
)

set_target_properties(cpc PROPERTIES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_sources(cpc
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_union.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_common.hpp
  PRIVATE
    src/compressionData.data
    src/cpc_sketch.cpp
    src/fm85.cpp
    src/fm85Compression.cpp
    src/fm85Confidence.cpp
    src/fm85Merging.cpp
    src/fm85Util.cpp
    src/iconEstimator.cpp
    src/u32Table.cpp
    include/common.h
    include/fm85.h
    include/fm85Compression.h
    include/fm85Confidence.h
    include/fm85Merging.h
    include/fm85Util.h
    include/iconEstimator.h
    include/u32Table.h
)
