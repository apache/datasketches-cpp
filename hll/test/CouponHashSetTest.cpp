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
#include "CouponHashSet.hpp"
#include "HllUtil.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ostream>
#include <cmath>
#include <string>
#include <exception>

namespace datasketches {

class CouponHashSetTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(CouponHashSetTest);
  CPPUNIT_TEST(checkCorruptBytearray);
  CPPUNIT_TEST(checkCorruptStream);
  CPPUNIT_TEST_SUITE_END();

  void checkCorruptBytearray() {
    int lgK = 8;
    hll_sketch sk1(lgK);
    for (int i = 0; i < 24; ++i) {
      sk1.update(i);
    }
    std::pair<std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>, const size_t> sketchBytes = sk1.serializeUpdatable();
    uint8_t* bytes = sketchBytes.first.get();

    bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = 0;
    // fail in HllSketchImpl
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 hll_sketch::deserialize(bytes, sketchBytes.second),
                                 std::invalid_argument);
    // fail in CouponHashSet
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 CouponHashSet<>::newSet(bytes, sketchBytes.second),
                                 std::invalid_argument);
    bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = HllUtil<>::HASH_SET_PREINTS;

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

    bytes[HllUtil<>::LG_K_BYTE] = 6;
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect too small a lgK for Set mode",
                                 hll_sketch::deserialize(bytes, sketchBytes.second),
                                 std::invalid_argument);
    bytes[HllUtil<>::LG_K_BYTE] = lgK;

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
    int lgK = 9;
    hll_sketch sk1(lgK);
    for (int i = 0; i < 24; ++i) {
      sk1.update(i);
    }
    std::stringstream ss;
    sk1.serializeCompact(ss);

    ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
    ss.put(0);
    ss.seekg(0);
    // fail in HllSketchImpl
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 hll_sketch::deserialize(ss),
                                 std::invalid_argument);
    // fail in CouponHashSet
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect error in preInts byte",
                                 CouponHashSet<>::newSet(ss),
                                 std::invalid_argument);
    ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
    ss.put(HllUtil<>::HASH_SET_PREINTS);

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
    ss.put(0x22); // HLL_8, HLL
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

CPPUNIT_TEST_SUITE_REGISTRATION(CouponHashSetTest);

} // namespace datasketches
