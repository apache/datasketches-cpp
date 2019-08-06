add_library(hll INTERFACE)

add_library(${PROJECT_NAME}::HLL ALIAS hll)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(hll
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_link_libraries(hll INTERFACE common)
target_compile_features(hll INTERFACE cxx_std_11)

# TODO: would be useful if this didn't need to be reproduced in target_sources(), too
set(hll_HEADERS "")
list(APPEND hll_HEADERS "include/hll.hpp;include/AuxHashMap.hpp;include/CompositeInterpolationXTable.hpp")
list(APPEND hll_HEADERS "include/CouponHashSet.hpp;include/CouponList.hpp")
list(APPEND hll_HEADERS "include/CubicInterpolation.hpp;include/HarmonicNumbers.hpp;include/Hll4Array.hpp")
list(APPEND hll_HEADERS "include/Hll6Array.hpp;include/Hll8Array.hpp;include/HllArray.hpp")
list(APPEND hll_HEADERS "include/HllPairIterator.hpp;include/HllSketchImpl.hpp")
list(APPEND hll_HEADERS "include/HllUtil.hpp;include/IntArrayPairIterator.hpp")
list(APPEND hll_HEADERS "include/PairIterator.hpp;include/RelativeErrorTables.hpp;include/AuxHashMap-internal.hpp")
list(APPEND hll_HEADERS "include/CompositeInterpolationXTable-internal.hpp")
list(APPEND hll_HEADERS "include/CouponHashSet-internal.hpp;include/CouponList-internal.hpp")
list(APPEND hll_HEADERS "include/CubicInterpolation-internal.hpp;include/HarmonicNumbers-internal.hpp")
list(APPEND hll_HEADERS "include/Hll4Array-internal.hpp;include/Hll6Array-internal.hpp")
list(APPEND hll_HEADERS "include/Hll8Array-internal.hpp;include/HllArray-internal.hpp")
list(APPEND hll_HEADERS "include/HllPairIterator-internal.hpp;include/HllSketch-internal.hpp")
list(APPEND hll_HEADERS "include/HllSketchImpl-internal.hpp;include/HllUnion-internal.hpp")
list(APPEND hll_HEADERS "include/IntArrayPairIterator-internal.hpp;include/RelativeErrorTables-internal.hpp")

install(TARGETS hll
  EXPORT ${PROJECT_NAME}
)

install(FILES ${hll_HEADERS}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")

target_sources(hll
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/hll.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/AuxHashMap.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CompositeInterpolationXTable.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CouponHashSet.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CouponList.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CubicInterpolation.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HarmonicNumbers.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Hll4Array.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Hll6Array.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Hll8Array.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllArray.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllPairIterator.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllSketchImpl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllUtil.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/IntArrayPairIterator.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/PairIterator.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/RelativeErrorTables.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/AuxHashMap-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CompositeInterpolationXTable-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CouponHashSet-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CouponList-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/CubicInterpolation-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HarmonicNumbers-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Hll4Array-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Hll6Array-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Hll8Array-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllArray-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllPairIterator-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllSketch-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllSketchImpl-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/HllUnion-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/IntArrayPairIterator-internal.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/RelativeErrorTables-internal.hpp
)
