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
//#include "HllArray.hpp"
//#include "HllSketch.hpp"

#include <exception>
#include <sstream>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class HllArrayTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(HllArrayTest);
  CPPUNIT_TEST(checkCompositeEstimate);
  CPPUNIT_TEST(checkSerializeDeserialize);
  CPPUNIT_TEST(checkIsCompact);
  CPPUNIT_TEST(checkCorruptBytearray);
  CPPUNIT_TEST(checkCorruptStream);
  CPPUNIT_TEST_SUITE_END();

  void testComposite(const int lgK, const TgtHllType tgtHllType, const int n) {
    hll_union u(lgK);
    hll_sketch sk(lgK, tgtHllType);
    for (int i = 0; i < n; ++i) {
      u.update(i);
      sk.update(i);
    }
    u.update(sk); // merge
    hll_sketch res = u.getResult(TgtHllType::HLL_8);
    double est = res.getCompositeEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est, sk.getCompositeEstimate(), 0.0);
  }

  void checkCompositeEstimate() {
    testComposite(4, TgtHllType::HLL_8, 10000);
    testComposite(5, TgtHllType::HLL_8, 10000);
    testComposite(6, TgtHllType::HLL_8, 10000);
    testComposite(13, TgtHllType::HLL_8, 10000);
  }

  void checkSerializeDeserialize() {
    int lgK = 4;
    int n = 8;
    serializeDeserialize(lgK, HLL_4, n);
    serializeDeserialize(lgK, HLL_6, n);
    serializeDeserialize(lgK, HLL_8, n);

    lgK = 15;
    n = (((1 << (lgK - 3))*3)/4) + 100;
    serializeDeserialize(lgK, HLL_4, n);
    serializeDeserialize(lgK, HLL_6, n);
    serializeDeserialize(lgK, HLL_8, n);

    lgK = 21;
    n = (((1 << (lgK - 3))*3)/4) + 1000;
    serializeDeserialize(lgK, HLL_4, n);
    serializeDeserialize(lgK, HLL_6, n);
    serializeDeserialize(lgK, HLL_8, n);
  }

  void serializeDeserialize(const int lgK, TgtHllType tgtHllType, const int n) {
    hll_sketch sk1(lgK, tgtHllType);

    for (int i = 0; i < n; ++i) {
      sk1.update(i);
    }
    //CPPUNIT_ASSERT(sk1.getCurrentMode() == CurMode::HLL);

    double est1 = sk1.getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, est1, n * 0.03);

    // serialize as compact and updatable, deserialize, compare estimates are exact
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk1.serializeCompact(ss);
    hll_sketch sk2 = hll_sketch::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk2.getEstimate(), sk1.getEstimate(), 0.0);

    ss.clear();
    sk1.serializeUpdatable(ss);
    sk2 = hll_sketch::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk2.getEstimate(), sk1.getEstimate(), 0.0);

    sk1.reset();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, sk1.getEstimate(), 0.0);
  }

  void checkIsCompact() {
    hll_sketch sk(4);
    for (int i = 0; i < 8; ++i) {
      sk.update(i);
    }
    CPPUNIT_ASSERT(!sk.isCompact());
  }

  void checkCorruptBytearray() {
    int lgK = 8;
    hll_sketch sk1(lgK, HLL_8);
    for (int i = 0; i < 50; ++i) {
      sk1.update(i);
    }
    std::pair<byte_ptr_with_deleter, size_t> sketchBytes = sk1.serializeCompact();
    uint8_t* bytes = sketchBytes.first.get();

    bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = 0;
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 hll_sketch::deserialize(bytes, sketchBytes.second),
                                 std::invalid_argument);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 HllArray<>::newHll(bytes, sketchBytes.second),
                                 std::invalid_argument);
    bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = HllUtil<>::HLL_PREINTS;

    bytes[HllUtil<>::SER_VER_BYTE] = 0;
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in serialization version byte",
                                 hll_sketch::deserialize(bytes, sketchBytes.second),
                                 std::invalid_argument);
    bytes[HllUtil<>::SER_VER_BYTE] = HllUtil<>::SER_VER;

    bytes[HllUtil<>::FAMILY_BYTE] = 0;
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in family id byte",
                                 hll_sketch::deserialize(bytes, sketchBytes.second),
                                 std::invalid_argument);
    bytes[HllUtil<>::FAMILY_BYTE] = HllUtil<>::FAMILY_ID;

    uint8_t tmp = bytes[HllUtil<>::MODE_BYTE];
    bytes[HllUtil<>::MODE_BYTE] = 0x10; // HLL_6, LIST
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in mode byte",
                                 hll_sketch::deserialize(bytes, sketchBytes.second),
                                 std::invalid_argument);
    bytes[HllUtil<>::MODE_BYTE] = tmp;

    tmp = bytes[HllUtil<>::LG_ARR_BYTE];
    bytes[HllUtil<>::LG_ARR_BYTE] = 0;
    hll_sketch::deserialize(bytes, sketchBytes.second);
    // should work fine despite the corruption
    bytes[HllUtil<>::LG_ARR_BYTE] = tmp;

    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in serialized length",
                                 hll_sketch::deserialize(bytes, sketchBytes.second - 1),
                                 std::invalid_argument);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in serialized length",
                                 hll_sketch::deserialize(bytes, 3),
                                 std::invalid_argument);
    }

  void checkCorruptStream() {
    int lgK = 6;
    hll_sketch sk1(lgK);
    for (int i = 0; i < 50; ++i) {
      sk1.update(i);
    }
    std::stringstream ss;
    sk1.serializeCompact(ss);

    ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
    ss.put(0);
    ss.seekg(0);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 hll_sketch::deserialize(ss),
                                 std::invalid_argument);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 HllArray<>::newHll(ss),
                                 std::invalid_argument);                                
    ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
    ss.put(HllUtil<>::HLL_PREINTS);

    ss.seekp(HllUtil<>::SER_VER_BYTE);
    ss.put(0);
    ss.seekg(0);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in serialization version byte",
                                 hll_sketch::deserialize(ss),
                                 std::invalid_argument);
    ss.seekp(HllUtil<>::SER_VER_BYTE);
    ss.put(HllUtil<>::SER_VER);

    ss.seekp(HllUtil<>::FAMILY_BYTE);
    ss.put(0);
    ss.seekg(0);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in family id byte",
                                 hll_sketch::deserialize(ss),
                                 std::invalid_argument);
    ss.seekp(HllUtil<>::FAMILY_BYTE);
    ss.put(HllUtil<>::FAMILY_ID);

    ss.seekg(HllUtil<>::MODE_BYTE);
    uint8_t tmp = ss.get();
    ss.seekp(HllUtil<>::MODE_BYTE);
    ss.put(0x11); // HLL_6, SET
    ss.seekg(0);
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in mode byte",
                                 hll_sketch::deserialize(ss),
                                 std::invalid_argument);
    ss.seekp(HllUtil<>::MODE_BYTE);
    ss.put(tmp);

    ss.seekg(HllUtil<>::LG_ARR_BYTE);
    tmp = ss.get();
    ss.seekp(HllUtil<>::LG_ARR_BYTE);
    ss.put(0);
    ss.seekg(0);
    hll_sketch::deserialize(ss);
    // should work fine despite the corruption
    ss.seekp(HllUtil<>::LG_ARR_BYTE);
    ss.put(tmp);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(HllArrayTest);

} /* namespace datasketches */
