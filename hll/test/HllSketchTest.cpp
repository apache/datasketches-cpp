/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "hll.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class hllSketchTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(hllSketchTest);
  CPPUNIT_TEST(checkCopies);
  CPPUNIT_TEST(checkCopyAs);
  CPPUNIT_TEST(checkMisc1);
  CPPUNIT_TEST(checkNumStdDev);
  CPPUNIT_TEST(checkSerSizes);
  CPPUNIT_TEST(exerciseToString);
  CPPUNIT_TEST(checkCompactFlag);
  CPPUNIT_TEST(checkKLimits);
  CPPUNIT_TEST(checkInputTypes);
  CPPUNIT_TEST_SUITE_END();

  void checkCopies() {
    runCheckCopy(14, HLL_4);
    runCheckCopy(8, HLL_6);
    runCheckCopy(8, HLL_8);
  }

  void runCheckCopy(int lgConfigK, target_hll_type tgtHllType) {
    hll_sketch sk(lgConfigK, tgtHllType);

    for (int i = 0; i < 7; ++i) {
      sk.update(i);
    }

    hll_sketch skCopy = sk;
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy.get_estimate(), sk.get_estimate(), 0.0);

    // no access to hllSketchImpl, so we'll ensure those differ by adding more
    // data to sk and ensuring the mode and estimates differ
    for (int i = 7; i < 24; ++i) {
      sk.update(i);
    }
    CPPUNIT_ASSERT(16.0 < (sk.get_estimate() - skCopy.get_estimate()));

    skCopy = sk;
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy.get_estimate(), sk.get_estimate(), 0.0);

    int u = (sk.get_target_type() == HLL_4) ? 100000 : 25;
    for (int i = 24; i < u; ++i) {
      sk.update(i);
    }
    CPPUNIT_ASSERT(sk.get_estimate() != skCopy.get_estimate()); // either 1 or 100k difference

    skCopy = sk;
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy.get_estimate(), sk.get_estimate(), 0.0);

  }

  void checkCopyAs() {
    copyAs(HLL_4, HLL_4);
    copyAs(HLL_4, HLL_6);
    copyAs(HLL_4, HLL_8);
    copyAs(HLL_6, HLL_4);
    copyAs(HLL_6, HLL_6);
    copyAs(HLL_6, HLL_8);
    copyAs(HLL_8, HLL_4);
    copyAs(HLL_8, HLL_6);
    copyAs(HLL_8, HLL_8);
  }

  void copyAs(target_hll_type srcType, target_hll_type dstType) {
    int lgK = 8;
    int n1 = 7;
    int n2 = 24;
    int n3 = 1000;
    int base = 0;

    hll_sketch src(lgK, srcType);
    for (int i = 0; i < n1; ++i) {
      src.update(i + base);
    }
    hll_sketch dst(src, dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst.get_estimate(), src.get_estimate(), 0.0);

    for (int i = n1; i < n2; ++i) {
      src.update(i + base);
    }
    dst = hll_sketch(src, dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst.get_estimate(), src.get_estimate(), 0.0);

    for (int i = n2; i < n3; ++i) {
      src.update(i + base);
    }
    dst = hll_sketch(src, dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst.get_estimate(), src.get_estimate(), 0.0);
  }

  void checkMisc1() {
    int lgConfigK = 8;
    target_hll_type srcType = target_hll_type::HLL_8;
    hll_sketch sk(lgConfigK, srcType);

    for (int i = 0; i < 7; ++i) { sk.update(i); } // LIST
    CPPUNIT_ASSERT_EQUAL(36, sk.get_compact_serialization_bytes());
    CPPUNIT_ASSERT_EQUAL(40, sk.get_updatable_serialization_bytes());

    for (int i = 7; i < 24; ++i) { sk.update(i); } // SET
    CPPUNIT_ASSERT_EQUAL(108, sk.get_compact_serialization_bytes());
    CPPUNIT_ASSERT_EQUAL(140, sk.get_updatable_serialization_bytes());

    sk.update(24); // HLL
    CPPUNIT_ASSERT_EQUAL(40 + 256, sk.get_updatable_serialization_bytes());

    const int hllBytes = HllUtil<>::HLL_BYTE_ARR_START + (1 << lgConfigK);
    CPPUNIT_ASSERT_EQUAL(hllBytes, sk.get_compact_serialization_bytes());
    CPPUNIT_ASSERT_EQUAL(hllBytes, hll_sketch::get_max_updatable_serialization_bytes(lgConfigK, HLL_8));
  }

  void checkNumStdDev() {
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw on invalid number of std deviations",
                                 HllUtil<>::checkNumStdDev(0), std::invalid_argument);
  }

  void checkSerSizes() {
    checkSerializationSizes(8, HLL_8);
    checkSerializationSizes(8, HLL_6);
    checkSerializationSizes(8, HLL_4);
  }

  void checkSerializationSizes(const int lgConfigK, target_hll_type tgtHllType) {
    hll_sketch sk(lgConfigK, tgtHllType);
    int i;

    // LIST
    for (i = 0; i < 7; ++i) { sk.update(i); }
    int expected = HllUtil<>::LIST_INT_ARR_START + (i << 2);
    CPPUNIT_ASSERT_EQUAL(expected, sk.get_compact_serialization_bytes());
    expected = HllUtil<>::LIST_INT_ARR_START + (4 << HllUtil<>::LG_INIT_LIST_SIZE);
    CPPUNIT_ASSERT_EQUAL(expected, sk.get_updatable_serialization_bytes());

    // SET
    for (i = 7; i < 24; ++i) { sk.update(i); }
    expected = HllUtil<>::HASH_SET_INT_ARR_START + (i << 2);
    CPPUNIT_ASSERT_EQUAL(expected, sk.get_compact_serialization_bytes());
    expected = HllUtil<>::HASH_SET_INT_ARR_START + (4 << HllUtil<>::LG_INIT_SET_SIZE);
    CPPUNIT_ASSERT_EQUAL(expected, sk.get_updatable_serialization_bytes());

    // HLL
    sk.update(i);
    //CPPUNIT_ASSERT_EQUAL(CurMode::HLL, s.getCurrentMode());
    //HllArray* hllArr = (HllArray*) s.hllSketchImpl;

  /*
    int auxCountBytes = 0;
    int auxArrBytes = 0;
    if (hllArr.getTgtHllType() == target_hll_type::HLL_4) {
      AuxHashMap* auxMap = hllArr.getAuxHashMap();
      if (auxMap != nullptr) {
        auxCountBytes = auxMap.getAuxCount() << 2;
        auxArrBytes = 4 << auxMap.getLgAuxArrInts();
      } else {
        auxArrBytes = 4 << HllUtil<>::LG_AUX_ARR_INTS[lgConfigK];
      }
    }
    int hllArrBytes = hllArr.getHllByteArrBytes();
    expected = HllUtil<>::HLL_BYTE_ARR_START + hllArrBytes + auxCountBytes;
    CPPUNIT_ASSERT_EQUAL(expected, sk.get_compact_serialization_bytes());
    expected = HllUtil<>::HLL_BYTE_ARR_START + hllArrBytes + auxArrBytes;
    CPPUNIT_ASSERT_EQUAL(expected, sk.get_updatable_serialization_bytes());
    
    int fullAuxArrBytes = (tgtHllType == HLL_4) ? (4 << HllUtil<>::LG_AUX_ARR_INTS[lgConfigK]) : 0;
    expected = HllUtil<>::HLL_BYTE_ARR_START + hllArrBytes + fullAuxArrBytes;
    CPPUNIT_ASSERT_EQUAL(expected,
                         HllSketch::get_max_updatable_serialization_bytes(lgConfigK, tgtHllType));
  */
  }

  void exerciseToString() {
    hll_sketch sk(15, HLL_4);
    for (int i = 0; i < 25; ++i) { sk.update(i); }
    std::ostringstream oss(std::ios::binary);
    sk.to_string(oss, false, true, true, true);
    for (int i = 25; i < (1 << 20); ++i) { sk.update(i); }
    sk.to_string(oss, false, true, true, true);
    sk.to_string(oss, false, true, true, false);

    // need to delete old sketch?
    sk = hll_sketch(8, HLL_8);
    for (int i = 0; i < 25; ++i) { sk.update(i); }
    sk.to_string(oss, false, true, true, true);
  }
  
  void checkCompactFlag() {
    int lgK = 8;
    // unless/until we create non-updatable "direct" versions,
    // deserialized image should never be compact
    // LIST: follows serialization request
    CPPUNIT_ASSERT(checkCompact(lgK, 7, HLL_8, false) == false);
    CPPUNIT_ASSERT(checkCompact(lgK, 7, HLL_8, true) == false);

    // SET: follows serialization request
    CPPUNIT_ASSERT(checkCompact(lgK, 24, HLL_8, false) == false);
    CPPUNIT_ASSERT(checkCompact(lgK, 24, HLL_8, true) == false);

    // HLL8: always updatable
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_8, false) == false);
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_8, true) == false);

    // HLL6: always updatable
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_6, false) == false);
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_6, true) == false);

    // HLL4: follows serialization request
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_4, false) == false);
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_4, true) == false);
  }

  // Creates and serializes then deserializes sketch.
  // Returns true if deserialized sketch is compact.
  bool checkCompact(const int lgK, const int n, const target_hll_type type, bool compact) {
    hll_sketch sk(lgK, type);
    for (int i = 0; i < n; ++i) { sk.update(i); }
    
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    if (compact) { 
      sk.serialize_compact(ss);
      CPPUNIT_ASSERT_EQUAL(sk.get_compact_serialization_bytes(), (int) ss.tellp());
    } else {
      sk.serialize_updatable(ss);
      CPPUNIT_ASSERT_EQUAL(sk.get_updatable_serialization_bytes(), (int) ss.tellp());
    }
    
    hll_sketch sk2 = hll_sketch::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, sk2.get_estimate(), 0.01);
    bool isCompact = sk2.is_compact();

    return isCompact;
  }

  void checkKLimits() {
    hll_sketch sketch1(HllUtil<>::MIN_LOG_K, target_hll_type::HLL_8);
    hll_sketch sketch2(HllUtil<>::MAX_LOG_K, target_hll_type::HLL_4);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw with lgK too small",
                                 hll_sketch(HllUtil<>::MIN_LOG_K - 1),
                                 std::invalid_argument);

    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw with lgK too large",
                                 hll_sketch(HllUtil<>::MAX_LOG_K + 1),
                                 std::invalid_argument);
  }

    void checkInputTypes() {
    hll_sketch sk(8, target_hll_type::HLL_8);

    // inserting the same value as a variety of input types
    sk.update((uint8_t) 102);
    sk.update((uint16_t) 102);
    sk.update((uint32_t) 102);
    sk.update((uint64_t) 102);
    sk.update((int8_t) 102);
    sk.update((int16_t) 102);
    sk.update((int32_t) 102);
    sk.update((int64_t) 102);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, sk.get_estimate(), 0.01);

    // identical binary representations
    // no unsigned in Java, but need to sign-extend both as Java would do 
    sk.update((uint8_t) 255);
    sk.update((int8_t) -1);

    sk.update((float) -2.0);
    sk.update((double) -2.0);

    std::string str = "input string";
    sk.update(str);
    sk.update(str.c_str(), str.length());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.0, sk.get_estimate(), 0.01);

    sk = hll_sketch(8, target_hll_type::HLL_6);
    sk.update((float) 0.0);
    sk.update((float) -0.0);
    sk.update((double) 0.0);
    sk.update((double) -0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, sk.get_estimate(), 0.01);

    sk = hll_sketch(8, target_hll_type::HLL_4);
    sk.update(std::nanf("3"));
    sk.update(std::nan("9"));
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, sk.get_estimate(), 0.01);

    sk = hll_sketch(8, target_hll_type::HLL_4);
    sk.update(nullptr, 0);
    sk.update("");
    CPPUNIT_ASSERT(sk.is_empty());
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(hllSketchTest);

} /* namespace datasketches */
