add_library(hll OBJECT "")

add_library(${PROJECT_NAME}::HLL ALIAS hll)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(hll
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_link_libraries(hll common)

set_target_properties(hll PROPERTIES
  PUBLIC_HEADER "include/hll.hpp"
  POSITION_INDEPENDENT_CODE ON
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

install(TARGETS hll
  EXPORT ${PROJCT_NAME}
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/DataSketches
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/DataSketches
)

target_sources(hll
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/hll.hpp
  PRIVATE
    include/AuxHashMap.hpp
    include/CompositeInterpolationXTable.hpp
    include/Conversions.hpp
    include/CouponHashSet.hpp
    include/CouponList.hpp
    include/CubicInterpolation.hpp
    include/HarmonicNumbers.hpp
    include/Hll4Array.hpp
    include/Hll6Array.hpp
    include/Hll8Array.hpp
    include/HllArray.hpp
    include/HllPairIterator.hpp
    include/HllSketch.hpp
    include/HllSketchImpl.hpp
    include/HllUnion.hpp
    include/HllUtil.hpp
    include/IntArrayPairIterator.hpp
    include/PairIterator.hpp
    include/RelativeErrorTables.hpp
    include/AuxHashMap-internal.hpp
    include/CompositeInterpolationXTable-internal.hpp
    include/Conversions-internal.hpp
    include/CouponHashSet-internal.hpp
    include/CouponList-internal.hpp
    include/CubicInterpolation-internal.hpp
    include/HarmonicNumbers-internal.hpp
    include/Hll4Array-internal.hpp
    include/Hll6Array-internal.hpp
    include/Hll8Array-internal.hpp
    include/HllArray-internal.hpp
    include/HllPairIterator-internal.hpp
    include/HllSketch-internal.hpp
    include/HllSketchImpl-internal.hpp
    include/HllUnion-internal.hpp
    include/IntArrayPairIterator-internal.hpp
    include/RelativeErrorTables-internal.hpp

)
