/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll.hpp"
#include "HllSketch.hpp"
#include "HllUnion.hpp"
#include "HllUtil.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ostream>
#include <cmath>
#include <string>

namespace datasketches {

class CouponListTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(CouponListTest);
  CPPUNIT_TEST(checkIterator);
  CPPUNIT_TEST(checkDuplicatesAndMisc);
  CPPUNIT_TEST(checkSerializeDeserialize);
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

};

CPPUNIT_TEST_SUITE_REGISTRATION(CouponListTest);

} /* namespace datasketches */
