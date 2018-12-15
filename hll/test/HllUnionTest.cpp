/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll.hpp"
#include "HllUnion.hpp"
#include "HllUtil.hpp"

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
  CPPUNIT_TEST(checkMisc);
  CPPUNIT_TEST(checkInputTypes);
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

    hll_sketch h1 = HllSketch::newInstance(lgk1, type1);
    hll_sketch h2 = HllSketch::newInstance(lgk2, type2);
    int lgControlK = min(min(lgk1, lgk2), lgMaxK);
    hll_sketch control = HllSketch::newInstance(lgControlK, resultType);

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

    hll_union u = HllUnion::newInstance(lgMaxK);
    u->update(*h1);
    u->update(*h2);

    hll_sketch result = u->getResult(resultType);
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
    hll_union srcU = HllUnion::newInstance(lgK);
    hll_sketch srcSk = HllSketch::newInstance(lgK, type);
    for (int i = 0; i < n; ++i) {
      srcSk->update(i);
    }
    srcU->update(*srcSk);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    if (compact) {
      srcU->serializeCompact(ss);
    } else {
      srcU->serializeUpdatable(ss);
    }
    hll_union dstU = HllUnion::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(srcU->getEstimate(), dstU->getEstimate(), 0.0);
  }

  void checkCompositeEstimate() {
    hll_union u = HllUnion::newInstance(12);
    CPPUNIT_ASSERT(u->isEmpty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, u->getCompositeEstimate(), 0.03);
    for (int i = 1; i <= 15; ++i) { u->update(i); }
    CPPUNIT_ASSERT_DOUBLES_EQUAL(15.0, u->getCompositeEstimate(), 15 * 0.03);
    for (int i = 16; i <= 1000; ++i) { u->update(i); }
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1000.0, u->getCompositeEstimate(), 1000 * 0.03);
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
    CPPUNIT_ASSERT_EQUAL(HLL_8, hllUnion->getTgtHllType());
    int bytes = hllUnion->getUpdatableSerializationBytes();
    CPPUNIT_ASSERT(bytes <= HllUnion::getMaxSerializationBytes(lgK));
    CPPUNIT_ASSERT_EQUAL(false, hllUnion->isCompact());
    
    delete hllUnion;
  }

  void checkConversions() {
    int lgK = 4;
    hll_sketch sk1 = HllSketch::newInstance(lgK, HLL_8);
    hll_sketch sk2 = HllSketch::newInstance(lgK, HLL_8);
    int n = 1 << 20;
    for (int i = 0; i < n; ++i) {
      sk1->update(i);
      sk2->update(i + n);
    }
    hll_union hllUnion = HllUnion::newInstance(lgK);
    hllUnion->update(*sk1);
    hllUnion->update(*sk2);

    std::unique_ptr<HllSketch> rsk1 = std::unique_ptr<HllSketch>(hllUnion->getResult(HLL_8));
    std::unique_ptr<HllSketch> rsk2 = std::unique_ptr<HllSketch>(hllUnion->getResult(HLL_8));
    std::unique_ptr<HllSketch> rsk3 = std::unique_ptr<HllSketch>(hllUnion->getResult(HLL_8));
    double est1 = rsk1->getEstimate();
    double est2 = rsk2->getEstimate();
    double est3 = rsk3->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, est2, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, est3, 0.0);
  }

  // moved from UnionCaseTest in java
  void checkMisc() {
    hll_union u = HllUnion::newInstance(12);
    int bytes = u->getCompactSerializationBytes();
    CPPUNIT_ASSERT_EQUAL(8, bytes);
    bytes = HllUnion::getMaxSerializationBytes(7);
    CPPUNIT_ASSERT_EQUAL(40 + 128, bytes);
    double v = u->getEstimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, v, 0.0);
    v = u->getLowerBound(1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, v, 0.0);
    v = u->getUpperBound(1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, v, 0.0);
    CPPUNIT_ASSERT(u->isEmpty());
    u->reset();
    CPPUNIT_ASSERT(u->isEmpty());
    std::ostringstream oss(std::ios::binary);
    u->serializeCompact(oss);
    CPPUNIT_ASSERT_EQUAL(8, static_cast<int>(oss.tellp()));
  }

  void checkInputTypes() {
    hll_union u = HllUnion::newInstance(8);

    // inserting the same value as a variety of input types
    u->update((uint8_t) 102);
    u->update((uint16_t) 102);
    u->update((uint32_t) 102);
    u->update((uint64_t) 102);
    u->update((int8_t) 102);
    u->update((int16_t) 102);
    u->update((int32_t) 102);
    u->update((int64_t) 102);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, u->getEstimate(), 0.01);

    // identical binary representations
    // no unsigned in Java, but need to sign-extend both as Java would do 
    u->update((uint8_t) 255);
    u->update((int8_t) -1);

    u->update((float) -2.0);
    u->update((double) -2.0);

    std::string str = "input string";
    u->update(str);
    u->update(str.c_str(), str.length());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.0, u->getEstimate(), 0.01);

    u = HllUnion::newInstance(8);
    u->update((float) 0.0);
    u->update((float) -0.0);
    u->update((double) 0.0);
    u->update((double) -0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, u->getEstimate(), 0.01);

    u = HllUnion::newInstance(8);
    u->update(std::nanf("3"));
    u->update(std::nan((char*)nullptr));
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, u->getEstimate(), 0.01);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(u->getResult()->getEstimate(), u->getEstimate(), 0.01);

    u = HllUnion::newInstance(8);
    u->update(nullptr, 0);
    u->update("");
    CPPUNIT_ASSERT(u->isEmpty());
  }
};

CPPUNIT_TEST_SUITE_REGISTRATION(HllUnionTest);

const int HllUnionTest::nArr[] = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

} /* namespace datasketches */
