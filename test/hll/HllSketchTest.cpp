/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "src/hll/hll.hpp"
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
  CPPUNIT_TEST(checkCopies);
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
    HllSketch* skContainer = HllSketch::newInstance(lgConfigK, tgtHllType);
    HllSketchPvt* sk = static_cast<HllSketchPvt*>(skContainer);

    for (int i = 0; i < 7; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::LIST);

    HllSketch* skCopy = sk->copy();
    HllSketchPvt* skCopyPvt = static_cast<HllSketchPvt*>(skCopy);
    CPPUNIT_ASSERT_EQUAL(skCopyPvt->getCurrentMode(), CurMode::LIST);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    // no access to hllSketchImpl, so we'll ensure those differ by adding more
    // data to sk and ensuring the mode and estimates differ
    for (int i = 7; i < 24; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::SET);
    CPPUNIT_ASSERT(sk->getCurrentMode() != skCopyPvt->getCurrentMode());
    std::cout << sk->getEstimate() << "\n";
    std::cout << skContainer->getEstimate() << "\n";
    std::cout << skCopyPvt->getEstimate() << "\n";
    std::cout << skCopy->getEstimate() << "\n";
    CPPUNIT_ASSERT_GREATER(16.0, sk->getEstimate() - skCopy->getEstimate());

    delete skCopy;
    skCopy = sk->copy();
    skCopyPvt = static_cast<HllSketchPvt*>(skCopy);
    CPPUNIT_ASSERT_EQUAL(skCopyPvt->getCurrentMode(), CurMode::SET);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    int u = (sk->getTgtHllType() == HLL_4) ? 100000 : 25;
    for (int i = 24; i < u; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT(sk->getCurrentMode() != skCopyPvt->getCurrentMode());
    CPPUNIT_ASSERT(sk->getEstimate() != skCopy->getEstimate()); // either 1 or 100k difference

    delete skCopy;
    skCopy = sk->copy();
    skCopyPvt = static_cast<HllSketchPvt*>(skCopy);
    CPPUNIT_ASSERT_EQUAL(skCopyPvt->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    CPPUNIT_ASSERT(sk->hllSketchImpl != skCopyPvt->hllSketchImpl);

    delete sk;
    delete skCopy;
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

  void copyAs(TgtHllType srcType, TgtHllType dstType) {
    int lgK = 8;
    int n1 = 7;
    int n2 = 24;
    int n3 = 1000;
    int base = 0;

    HllSketch* src = HllSketch::newInstance(lgK, srcType);
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
