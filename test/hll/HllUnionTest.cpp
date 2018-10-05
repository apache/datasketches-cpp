/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "src/hll/hll.hpp"
#include "src/hll/HllUnion.hpp"
#include "src/hll/HllUtil.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class HllUnionTest : public CppUnit::TestFixture {

  // list of values defined at bottom of file
  static const int nArr[]; // = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

  CPPUNIT_TEST_SUITE(HllUnionTest);
  CPPUNIT_TEST(checkUnions);
  CPPUNIT_TEST(checkToFrom);
  CPPUNIT_TEST(checkCompositeEstimate);
  CPPUNIT_TEST(checkConfigKLimits);
  CPPUNIT_TEST(checkUbLb);
  CPPUNIT_TEST(checkEmptyCoupon);
  CPPUNIT_TEST(checkConversions);
  CPPUNIT_TEST_SUITE_END();

  int min(int a, int b) {
    return (a < b) ? a : b;
  }

  void println(std::string& str) {
    //std::cout << str << "\n";
  }

  /**
   * The task here is to check the transition boundaries as the sketch morphs between LIST to
   * SET to HLL modes. The transition points vary as a function of lgConfigK. In addition,
   * this checks that the union operation is operating properly based on the order the
   * sketches are presented to the union.
   */
  void checkUnions() {
    TgtHllType type1 = HLL_8;
    TgtHllType type2 = HLL_8;
    TgtHllType resultType = HLL_8;

    int lgK1 = 7;
    int lgK2 = 7;
    int lgMaxK = 7;
    int n1 = 7;
    int n2 = 7;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    n1 = 8;
    n2 = 7;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    n1 = 7;
    n2 = 8;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    n1 = 8;
    n2 = 8;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    n1 = 7;
    n2 = 14;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);

    int i = 0;
    for (i = 7; i <= 13; ++i) {
      lgK1 = i;
      lgK2 = i;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4; // compute the transition point
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      }
      lgK1 = i;
      lgK2 = i + 1;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4;
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      }
      lgK1 = i + 1;
      lgK2 = i;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4;
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      }
      lgK1 = i + 1;
      lgK2 = i + 1;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4;
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      }
    }
  }

  void basicUnion(int n1, int n2, int lgk1, int lgk2, int lgMaxK,
                  TgtHllType type1, TgtHllType type2, TgtHllType resultType) {
    uint64_t v = 0;
    //int tot = n1 + n2;

    HllSketch* h1 = HllSketch::newInstance(lgk1, type1);
    HllSketch* h2 = HllSketch::newInstance(lgk2, type2);
    int lgControlK = min(min(lgk1, lgk2), lgMaxK);
    HllSketch* control = HllSketch::newInstance(lgControlK, resultType);

    for (uint64_t i = 0; i < n1; ++i) {
      h1->update(v + i);
      control->update(v + i);
    }
    v += n1;
    for (uint64_t i = 0; i < n2; ++i) {
      h2->update(v + i);
      control->update(v + i);
    }
    v += n2;

    HllUnion* u = HllUnion::newInstance(lgMaxK);
    u->update(h1);
    u->update(h2);

    HllSketch* result = u->getResult(resultType);
    //int lgkr = result->getLgConfigK();

    // force non-HIP estimates to avoid issues with in- vs out-of-order
    double uEst = result->getCompositeEstimate();
    double uUb = result->getUpperBound(2);
    double uLb = result->getLowerBound(2);
    //double rerr = ((uEst/tot) - 1.0) * 100;

    double controlEst = control->getCompositeEstimate();
    double controlUb = control->getUpperBound(2);
    double controlLb = control->getLowerBound(2);

    CPPUNIT_ASSERT((controlUb - controlEst) >= 0.0);
    CPPUNIT_ASSERT((uUb - uEst) >= 0.0);
    CPPUNIT_ASSERT((controlEst - controlLb) >= 0.0);
    CPPUNIT_ASSERT((uEst - uLb) >= 0.0);

    CPPUNIT_ASSERT_DOUBLES_EQUAL(controlEst, uEst, 0.0);

    delete h1;
    delete h2;
    delete control;
    delete u;
    delete result;
  }

  void checkToFrom() {
    for (int i = 0; i < 10; ++i) {
      int n = nArr[i];
      for (int lgK = 4; lgK <= 13; ++lgK) {
        toFrom(lgK, HLL_4, n, true);
        toFrom(lgK, HLL_6, n, true);
        toFrom(lgK, HLL_8, n, true);
        toFrom(lgK, HLL_4, n, false);
        toFrom(lgK, HLL_6, n, false);
        toFrom(lgK, HLL_8, n, false);
      }
    }
  }

  void toFrom(const int lgK, const TgtHllType type, const int n, const bool compact) {
    HllUnion* srcU = HllUnion::newInstance(lgK);
    HllSketch* srcSk = HllSketch::newInstance(lgK, type);
    for (int i = 0; i < n; ++i) {
      srcSk->update(i);
    }
    srcU->update(srcSk);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    if (compact) {
      srcU->serializeCompact(ss);
    } else {
      srcU->serializeUpdatable(ss);
    }
    HllUnion* dstU = HllUnion::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(srcU->getEstimate(), dstU->getEstimate(), 0.0);
    
    delete srcU;
    delete dstU;
    delete srcSk;
  }

  void checkCompositeEstimate() {
    HllUnion* u = HllUnion::newInstance(12);
    CPPUNIT_ASSERT(u->isEmpty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, u->getCompositeEstimate(), 0.03);
    for (int i = 1; i <= 15; ++i) { u->update(i); }
    CPPUNIT_ASSERT_DOUBLES_EQUAL(15.0, u->getCompositeEstimate(), 15 * 0.03);
    for (int i = 16; i <= 1000; ++i) { u->update(i); }
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1000.0, u->getCompositeEstimate(), 1000 * 0.03);
    
    delete u;
  }

  void checkConfigKLimits() {
    try {
      HllUnion::newInstance(HllUtil::MIN_LOG_K - 1);
      CPPUNIT_FAIL("Must fail: lgK too small");
    } catch (std::exception& e) {
      // expected
    }

    try {
      HllUnion::newInstance(HllUtil::MAX_LOG_K + 1);
      CPPUNIT_FAIL("Must fail: lgK too large");
    } catch (std::exception& e) {
      // expected
    }
  }

  double getBound(int lgK, bool ub, bool oooFlag, int numStdDev, double est) {
    double re = RelativeErrorTables::getRelErr(ub, oooFlag, lgK, numStdDev);
    return est / (1.0 + re);
  }

  void checkUbLb() { // for lgK <= 12
    int lgK = 4;
    int n = 1 << 20;
    bool oooFlag = false;
    
    double bound;
    std::string str;

    bound = (getBound(lgK, true, oooFlag, 3, n) / n) - 1;
    str = "LgK=" + std::to_string(lgK) + ", UB3: " + std::to_string(bound);
    println(str);
    bound = (getBound(lgK, true, oooFlag, 2, n) / n) - 1;
    str = "LgK=" + std::to_string(lgK) + ", UB2: " + std::to_string(bound);
    println(str);
    bound = (getBound(lgK, true, oooFlag, 1, n) / n) - 1;
    str = "LgK=" + std::to_string(lgK) + ", UB1: " + std::to_string(bound);
    println(str);
    bound = (getBound(lgK, false, oooFlag, 1, n) / n) - 1;
    str = "LgK=" + std::to_string(lgK) + ", LB1: " + std::to_string(bound);
    println(str);
    bound = (getBound(lgK, false, oooFlag, 2, n) / n) - 1;
    str = "LgK=" + std::to_string(lgK) + ", LB2: " + std::to_string(bound);
    println(str);
    bound = (getBound(lgK, false, oooFlag, 3, n) / n) - 1;
    str = "LgK=" + std::to_string(lgK) + ", LB3: " + std::to_string(bound);
    println(str);
  }

  void checkEmptyCoupon() {
    int lgK = 8;
    HllUnionPvt* hllUnion = new HllUnionPvt(lgK);
    for (int i = 0; i < 20; ++i) { hllUnion->update(i); } // SET mode
    hllUnion->couponUpdate(0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(20.0, hllUnion->getEstimate(), 0.001);
    CPPUNIT_ASSERT(hllUnion->getTgtHllType() == HLL_8);
    int bytes = hllUnion->getUpdatableSerializationBytes();
    CPPUNIT_ASSERT(bytes <= HllUnion::getMaxSerializationBytes(lgK));
    CPPUNIT_ASSERT(hllUnion->isCompact() == false);
    
    delete hllUnion;
  }

  void checkConversions() {
    int lgK = 4;
    HllSketch* sk1 = HllSketch::newInstance(lgK, HLL_8);
    HllSketch* sk2 = HllSketch::newInstance(lgK, HLL_8);
    int n = 1 << 20;
    for (int i = 0; i < n; ++i) {
      sk1->update(i);
      sk2->update(i + n);
    }
    HllUnion* hllUnion = HllUnion::newInstance(lgK);
    hllUnion->update(sk1);
    hllUnion->update(sk2);

    std::unique_ptr<HllSketch> rsk1 = std::unique_ptr<HllSketch>(hllUnion->getResult(HLL_8));
    std::unique_ptr<HllSketch> rsk2 = std::unique_ptr<HllSketch>(hllUnion->getResult(HLL_8));
    std::unique_ptr<HllSketch> rsk3 = std::unique_ptr<HllSketch>(hllUnion->getResult(HLL_8));
    double est1 = rsk1->getEstimate();
    double est2 = rsk2->getEstimate();
    double est3 = rsk3->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, est2, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, est3, 0.0);

    delete sk1;
    delete sk2;
    delete hllUnion;
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(HllUnionTest);

const int HllUnionTest::nArr[] = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

} /* namespace datasketches */
