/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll.hpp"
#include "HllSketch.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class HllArrayTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(HllArrayTest);
  CPPUNIT_TEST(checkCompositeEstimate);
  CPPUNIT_TEST(checkIsCompact);
  CPPUNIT_TEST_SUITE_END();

  void testComposite(const int lgK, const TgtHllType tgtHllType, const int n) {
    HllUnion* u = HllUnion::newInstance(lgK);
    HllSketch* sk = HllSketch::newInstance(lgK, tgtHllType);
    for (int i = 0; i < n; ++i) {
      u->update(i);
      sk->update(i);
    }
    u->update(sk); // merge
    HllSketch* res = u->getResult(TgtHllType::HLL_8);
    double est = res->getCompositeEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est, sk->getCompositeEstimate(), 0.0);

    delete res;
    delete u;
    delete sk;
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
    HllSketch* sk1 = HllSketch::newInstance(lgK, tgtHllType);

    for (int i = 0; i < n; ++i) {
      sk1->update(i);
    }
    CPPUNIT_ASSERT(((HllSketchPvt*)sk1)->getCurrentMode() == CurMode::HLL);

    double est1 = sk1->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, est1, n * 0.03);

    // serialize as compact and updatable, deserialize, compare estimates are exact
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk1->serializeCompact(ss);
    HllSketch* sk2 = HllSketch::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk2->getEstimate(), sk1->getEstimate(), 0.0);
    delete sk2;

    ss.clear();
    sk1->serializeUpdatable(ss);
    sk2 = HllSketch::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk2->getEstimate(), sk1->getEstimate(), 0.0);
    delete sk2;

    sk1->reset();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, sk1->getEstimate(), 0.0);
    
    delete sk1;
  }

  void checkIsCompact() {
    HllSketch* sk = HllSketch::newInstance(4);
    for (int i = 0; i < 8; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT(!sk->isCompact());
    delete sk;
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(HllArrayTest);

} /* namespace datasketches */
