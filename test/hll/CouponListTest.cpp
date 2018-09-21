/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "src/hll/HllSketch.hpp"
#include "src/hll/HllUnion.hpp"
#include "src/hll/HllUtil.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ostream>
#include <cmath>
#include <string>

namespace datasketches {

class couponListTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(couponListTest);
  CPPUNIT_TEST(checkIterator);
  CPPUNIT_TEST(checkDuplicatesAndMisc);
  CPPUNIT_TEST(checkSerializeDeserialize);
  CPPUNIT_TEST_SUITE_END();

  void println_string(const std::string str) {
    //std::cout << str << "\n";
  }

  void checkIterator() {
    int lgConfigK = 8;
    HllSketch* sk = new HllSketch(lgConfigK);
    for (int i = 0; i < 7; ++i) { sk->update(i); }
    std::unique_ptr<PairIterator> itr = sk->getIterator();
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

    delete sk;
  }

  void checkDuplicatesAndMisc() {
    int lgConfigK = 8;
    HllSketch* sk = new HllSketch(lgConfigK);

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

    delete sk;
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
    HllSketch* sk1 = new HllSketch(lgK);

    int u = (lgK < 8) ? 7 : (((1 << (lgK - 3))/ 4) * 3);
    for (int i = 0; i < u; ++i) {
      sk1->update(i);
    }
    double est1 = sk1->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, u, u * 100.0e-6);

    std::ostringstream oss;
    sk1->serializeCompact(oss);
    std::string bytes = oss.str();
    //std::cout << dumpAsHex(bytes.c_str(), bytes.length()) << "\n";;
    
    std::istringstream iss(bytes);
    HllSketch* sk2 = HllSketch::deserialize(iss);
    double est2 = sk2->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est2, est1, 0.0);
    delete sk2;

    oss.str(std::string());
    oss.clear();

    sk1->serializeUpdatable(oss);
    bytes = oss.str();
    iss.str(bytes);
    iss.clear();
    sk2 = HllSketch::deserialize(iss);
    est2 = sk2->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est2, est1, 0.0);

    delete sk1;
    delete sk2;
  }
  
  void checkSerializeDeserialize() {
    serializeDeserialize(7);
    serializeDeserialize(21);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(couponListTest);

} /* namespace datasketches */
