/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll.hpp"
#include "CouponList.hpp"
#include "HllSketch.hpp"
#include "HllUnion.hpp"
#include "HllUtil.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ostream>
#include <cmath>
#include <string>
#include <exception>

namespace datasketches {

class CouponListTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(CouponListTest);
  CPPUNIT_TEST(checkIterator);
  CPPUNIT_TEST(checkDuplicatesAndMisc);
  CPPUNIT_TEST(checkSerializeDeserialize);
  CPPUNIT_TEST(checkCorruptBytearrayData);
  CPPUNIT_TEST(checkCorruptStreamData);
  CPPUNIT_TEST_SUITE_END();

  void println_string(std::string str) {
    //std::cout << str << "\n";
  }

  void checkIterator() {
    int lgConfigK = 8;
    hll_sketch sk = HllSketch::newInstance(lgConfigK);
    for (int i = 0; i < 7; ++i) { sk->update(i); }
    std::unique_ptr<PairIterator> itr = static_cast<HllSketchPvt*>(sk.get())->getIterator();
    println_string(itr->getHeader());
    while (itr->nextAll()) {
      int key = itr->getKey();
      int val = itr->getValue();
      int idx = itr->getIndex();
      int slot = itr->getSlot();
      std::ostringstream oss;
      oss << "Idx: " << idx << ", Key: " << key << ", Val: " << val
          << ", Slot: " << slot;
      println_string(oss.str());
    }
  }

  void checkDuplicatesAndMisc() {
    int lgConfigK = 8;
    hll_sketch skContainer = HllSketch::newInstance(lgConfigK);
    HllSketchPvt* sk = static_cast<HllSketchPvt*>(skContainer.get());

    for (int i = 1; i <= 7; ++i) {
      sk->update(i);
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::LIST);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getCompositeEstimate(), 7.0, 7 * 0.1);

    sk->update(8);
    sk->update(8);
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::SET);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getCompositeEstimate(), 8.0, 8 * 0.1);

    for (int i = 9; i <= 25; ++i) {
      sk->update(i);
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getCompositeEstimate(), 25.0, 25 * 0.1);

    double relErr = sk->getRelErr(true, true, 4, 1);
    CPPUNIT_ASSERT(relErr < 0.0);
  }

  std::string dumpAsHex(const char* data, int len) {
   constexpr uint8_t hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                 '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    std::string s(len * 2, ' ');
    for (int i = 0; i < len; ++i) {
      s[2 * i]     = hexmap[(data[i] & 0xF0) >> 4];
      s[2 * i + 1] = hexmap[(data[i] & 0x0F)];
    }
    return s;
  }

  void serializeDeserialize(const int lgK) {
    hll_sketch sk1 = HllSketch::newInstance(lgK);

    int u = (lgK < 8) ? 7 : (((1 << (lgK - 3))/ 4) * 3);
    for (int i = 0; i < u; ++i) {
      sk1->update(i);
    }
    double est1 = sk1->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, u, u * 1e-4);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk1->serializeCompact(ss);
    hll_sketch sk2 = HllSketch::deserialize(ss);
    double est2 = sk2->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est2, est1, 0.0);

    ss.str(std::string());
    ss.clear();

    sk1->serializeUpdatable(ss);
    sk2 = HllSketch::deserialize(ss);
    est2 = sk2->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est2, est1, 0.0);
  }
  
  void checkSerializeDeserialize() {
    serializeDeserialize(7);
    serializeDeserialize(21);
  }

  void checkCorruptBytearrayData() {
    int lgK = 6;
    hll_sketch sk1 = HllSketch::newInstance(lgK);
    sk1->update(1);
    sk1->update(2);
    std::pair<std::unique_ptr<uint8_t>, size_t> sketchBytes = sk1->serializeCompact();
    uint8_t* bytes = sketchBytes.first.get();

    bytes[HllUtil::PREAMBLE_INTS_BYTE] = 0;
    try {
      // fail in HllSketchImpl
      HllSketch::deserialize(bytes, sketchBytes.second);
      CPPUNIT_FAIL("Failed to detect error in preInts byte");
    } catch (std::exception e) {
      // expected
    }

    try {
      // fail in CouponList
      CouponList::newList(bytes, sketchBytes.second);
      CPPUNIT_FAIL("Failed to detect error in preInts byte");
    } catch (std::exception e) {
      // expected
    }
    bytes[HllUtil::PREAMBLE_INTS_BYTE] = HllUtil::LIST_PREINTS;

    bytes[HllUtil::SER_VER_BYTE] = 0;
    try {
      HllSketch::deserialize(bytes, sketchBytes.second);
      CPPUNIT_FAIL("Failed to detect error in serialization version byte");
    } catch (std::exception e) {
      // expected
    }
    bytes[HllUtil::SER_VER_BYTE] = HllUtil::SER_VER;

    bytes[HllUtil::FAMILY_BYTE] = 0;
    try {
      HllSketch::deserialize(bytes, sketchBytes.second);
      CPPUNIT_FAIL("Failed to detect error in family id byte");
    } catch (std::exception e) {
      // expected
    }
    bytes[HllUtil::FAMILY_BYTE] = HllUtil::FAMILY_ID;

    uint8_t tmp = bytes[HllUtil::MODE_BYTE];
    bytes[HllUtil::MODE_BYTE] = 0x01; // HLL_4, SET
    try {
      HllSketch::deserialize(bytes, sketchBytes.second);
      CPPUNIT_FAIL("Failed to detect error in mode byte");
    } catch (std::exception e) {
      // expected
    }
    bytes[HllUtil::MODE_BYTE] = tmp;

    try {
      HllSketch::deserialize(bytes, sketchBytes.second - 1);
      CPPUNIT_FAIL("Failed to detect error in serialized length");
    } catch (std::exception e) {
      // expected
    }

    try {
      HllSketch::deserialize(bytes, 3);
      CPPUNIT_FAIL("Failed to detect error in serialized length");
    } catch (std::exception e) {
      // expected
    }
  }

  void checkCorruptStreamData() {
    int lgK = 6;
    hll_sketch sk1 = HllSketch::newInstance(lgK);
    sk1->update(1);
    sk1->update(2);
    std::stringstream ss;
    sk1->serializeCompact(ss);

    ss.seekp(HllUtil::PREAMBLE_INTS_BYTE);
    ss.put(0);
    ss.seekg(0);
    try {
      // fail in HllSketchImpl
      HllSketch::deserialize(ss);
      CPPUNIT_FAIL("Failed to detect error in preInts byte");
    } catch (std::exception e) {
      // expected
    }

    try {
      // fail in CouponList
      CouponList::newList(ss);
      CPPUNIT_FAIL("Failed to detect error in preInts byte");
    } catch (std::exception e) {
      // expected
    }
    ss.seekp(HllUtil::PREAMBLE_INTS_BYTE);
    ss.put(HllUtil::LIST_PREINTS);

    ss.seekp(HllUtil::SER_VER_BYTE);
    ss.put(0);
    ss.seekg(0);
    try {
      HllSketch::deserialize(ss);
      CPPUNIT_FAIL("Failed to detect error in serialization version byte");
    } catch (std::exception e) {
      // expected
    }
    ss.seekp(HllUtil::SER_VER_BYTE);
    ss.put(HllUtil::SER_VER);

    ss.seekp(HllUtil::FAMILY_BYTE);
    ss.put(0);
    ss.seekg(0);
    try {
      HllSketch::deserialize(ss);
      CPPUNIT_FAIL("Failed to detect error in family id byte");
    } catch (std::exception e) {
      // expected
    }
    ss.seekp(HllUtil::FAMILY_BYTE);
    ss.put(HllUtil::FAMILY_ID);

    ss.seekp(HllUtil::MODE_BYTE);
    ss.put(0x22); // HLL_8, HLL
    ss.seekg(0);
    try {
      HllSketch::deserialize(ss);
      CPPUNIT_FAIL("Failed to detect error in mode byte");
    } catch (std::exception e) {
      // expected
    }
    // end of test so not undoing change
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(CouponListTest);

} /* namespace datasketches */
