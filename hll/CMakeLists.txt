add_library(hll OBJECT "")
add_executable(hll_test)

target_include_directories(hll
  PUBLIC
    include
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_include_directories(hll_test
  PRIVATE
    include
    ${COMMON_INCLUDE_DIR}
    ${CPPUNIT_INCLUDE_DIR}
)

set(HLL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(hll_test hll common_test)

set_target_properties(hll PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO   # on Mac, sues -std=c++11 rather than -std=gnu++11
)

set_target_properties(hll_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO   # on Mac, sues -std=c++11 rather than -std=gnu++11
)


target_sources_local(hll
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include/hll.hpp
  PRIVATE
    src/AuxHashMap.cpp
    src/CompositeInterpolationXTable.cpp
    src/Conversions.cpp
    src/CouponHashSet.cpp
    src/CouponList.cpp
    src/CubicInterpolation.cpp
    src/HarmonicNumbers.cpp
    src/Hll4Array.cpp
    src/Hll6Array.cpp
    src/Hll8Array.cpp
    src/HllArray.cpp
    src/HllPairIterator.cpp
    src/HllSketch.cpp
    src/HllSketchImpl.cpp
    src/HllUnion.cpp
    src/HllUtil.cpp
    src/IntArrayPairIterator.cpp
    src/RelativeErrorTables.cpp
    # headers
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
)

target_sources_local(hll_test
  PRIVATE
    test/AuxHashMapTest.cpp
    test/CouponHashSetTest.cpp
    test/CouponListTest.cpp
    test/CrossCountingTest.cpp
    test/HllArrayTest.cpp
    test/HllSketchTest.cpp
    test/HllUnionTest.cpp
    test/TablesTest.cpp
    test/ToFromByteArrayTest.cpp
    test/UnionCaseTest.cpp
)
