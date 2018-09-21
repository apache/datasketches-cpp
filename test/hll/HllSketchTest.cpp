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

class hllSketchTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(hllSketchTest);
  //CPPUNIT_TEST(checkCopies);
  CPPUNIT_TEST(checkCopyAs);
  //CPPUNIT_TEST(checkMisc1);
  //CPPUNIT_TEST(checkNumStdDev);
  //CPPUNIT_TEST(checkSerSizes);
  //CPPUNIT_TEST(checkConfigKLimits);
  //CPPUNIT_TEST(exerciseToString);
  //CPPUNIT_TEST(checkEmptyCoupon);
  //CPPUNIT_TEST(checkCompactFlag);
  CPPUNIT_TEST_SUITE_END();

  void checkCopies() {
    runCheckCopy(14, HLL_4);
    runCheckCopy(8, HLL_6);
    runCheckCopy(8, HLL_8);
  }

  void runCheckCopy(int lgConfigK, TgtHllType tgtHllType) {
    HllSketch* sk = new HllSketch(lgConfigK, tgtHllType);

    for (int i = 0; i < 7; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::LIST);

    HllSketch* skCopy = sk->copy();
    CPPUNIT_ASSERT_EQUAL(skCopy->getCurrentMode(), CurMode::LIST);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    // no access to hllSketchImpl, so we'll ensure those differ by adding more
    // data to sk and ensuring the mode and estimates differ
    for (int i = 7; i < 24; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::SET);
    CPPUNIT_ASSERT(sk->getCurrentMode() != skCopy->getCurrentMode());
    CPPUNIT_ASSERT_GREATER(16.0, sk->getEstimate() - skCopy->getEstimate());

    delete skCopy;
    skCopy = sk->copy();
    CPPUNIT_ASSERT_EQUAL(skCopy->getCurrentMode(), CurMode::SET);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    int u = (sk->getTgtHllType() == HLL_4) ? 100000 : 25;
    for (int i = 24; i < u; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT(sk->getCurrentMode() != skCopy->getCurrentMode());
    CPPUNIT_ASSERT(sk->getEstimate() != skCopy->getEstimate()); // either 1 or 100k difference

    delete skCopy;
    skCopy = sk->copy();
    CPPUNIT_ASSERT_EQUAL(skCopy->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    // TODO: can we check that the impls differ?

    delete sk;
    delete skCopy;
  }

  void checkCopyAs() {
    /*
    int lgK = 4;
    HllSketch* sk1 = new HllSketch(lgK, HLL_4);
    HllSketch* sk2 = new HllSketch(lgK, HLL_6);

    for (int i = 0; i < 7; ++i) {
      sk1->update(i);
      sk2->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), CurMode::LIST);
    CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), sk2->getCurrentMode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1->getEstimate(), sk2->getEstimate(), 0.0);

    for (int i = 7; i < 24; ++i) {
      sk1->update(i);
      sk2->update(i);
    }
    //CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), CurMode::SET);
    CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), sk2->getCurrentMode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1->getEstimate(), sk2->getEstimate(), 0.0);

    //for (int i = 24; sk1->getCurrentMode() != HLL; ++i) {
    for (int i = 24; i < 100000; ++i) {
      sk1->update(i);
      sk2->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), sk2->getCurrentMode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1->getEstimate(), sk2->getEstimate(), 0.0);

    sk1->to_string(std::cout, true, true, true, true);
    sk2->to_string(std::cout, true, true, true, true);

    delete sk1;
    delete sk2;
    */
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

  void copyAs(TgtHllType srcType, TgtHllType dstType) {
    int lgK = 8;
    int n1 = 7;
    int n2 = 24;
    int n3 = 1000;
    int base = 0;
    std::cerr << srcType << "\t" << dstType << "\n"; std::cerr.flush();
    HllSketch* src = new HllSketch(lgK, srcType);
    for (int i = 0; i < n1; ++i) {
      src->update(i + base);
    }
    HllSketch* dst = src->copyAs(dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst->getEstimate(), src->getEstimate(), 0.0);
    delete dst;

    for (int i = n1; i < n2; ++i) {
      src->update(i + base);
    }
    dst = src->copyAs(dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst->getEstimate(), src->getEstimate(), 0.0);
    delete dst;

    for (int i = n2; i < n3; ++i) {
      src->update(i + base);
    }
    dst = src->copyAs(dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst->getEstimate(), src->getEstimate(), 0.0);
    delete dst;

    delete src;
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(hllSketchTest);

} /* namespace datasketches */
