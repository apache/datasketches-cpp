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

class CrossCountingTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(CrossCountingTest);
  CPPUNIT_TEST(crossCountingChecks);
  CPPUNIT_TEST_SUITE_END();

  HllSketch* buildSketch(const int n, const int lgK, const TgtHllType tgtHllType) {
    HllSketch* sketch = HllSketch::newInstance(lgK, tgtHllType);
    for (int i = 0; i < n; ++i) {
      sketch->update(i);
    }
    return sketch;
  }

  int computeChecksum(HllSketch* sketch) {
    HllSketchPvt* sk = (HllSketchPvt*) sketch;
    std::unique_ptr<PairIterator> itr = sk->getIterator();
    int checksum = 0;
    int key = 0;
    while(itr->nextAll()) {
      checksum += itr->getPair();
      key = itr->getKey(); // dummy
    }
    return checksum;
  }

  void crossCountingCheck(const int lgK, const int n) {
    HllSketch* sk4 = buildSketch(n, lgK, HLL_4);
    int s4csum = computeChecksum(sk4);
    int csum;

    HllSketch* sk6 = buildSketch(n, lgK, HLL_6);
    csum = computeChecksum(sk6);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);

    HllSketch* sk8 = buildSketch(n, lgK, HLL_8);
    csum = computeChecksum(sk8);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);
  
    // Conversions
    HllSketch* sk4to6 = sk4->copyAs(HLL_6);
    csum = computeChecksum(sk4to6);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);
    delete sk4to6;

    HllSketch* sk4to8 = sk4->copyAs(HLL_8);
    csum = computeChecksum(sk4to8);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);
    delete sk4to8;

    HllSketch* sk6to4 = sk6->copyAs(HLL_4);
    csum = computeChecksum(sk6to4);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);
    delete sk6to4;

    HllSketch* sk6to8 = sk6->copyAs(HLL_8);
    csum = computeChecksum(sk6to8);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);
    delete sk6to8;

    HllSketch* sk8to4 = sk8->copyAs(HLL_4);
    csum = computeChecksum(sk8to4);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);
    delete sk8to4;

    HllSketch* sk8to6 = sk8->copyAs(HLL_6);
    csum = computeChecksum(sk8to6);
    CPPUNIT_ASSERT_EQUAL(csum, s4csum);
    delete sk8to6;

    delete sk4;
    delete sk6;
    delete sk8;
  }

  void crossCountingChecks() {
    crossCountingCheck(4, 100);
    crossCountingCheck(4, 10000);
    crossCountingCheck(12, 7);
    crossCountingCheck(12, 384);
    crossCountingCheck(12, 10000);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(CrossCountingTest);

} /* namespace datasketches */
