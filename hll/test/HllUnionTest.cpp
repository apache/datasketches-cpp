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
  //CPPUNIT_TEST(checkEmptyCoupon);
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
    target_hll_type type1 = HLL_8;
    target_hll_type type2 = HLL_8;
    target_hll_type resultType = HLL_8;

    uint64_t lgK1 = 7;
    uint64_t lgK2 = 7;
    uint64_t lgMaxK = 7;
    uint64_t n1 = 7;
    uint64_t n2 = 7;
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

  void basicUnion(uint64_t n1, uint64_t n2,
		  uint64_t lgk1, uint64_t lgk2, uint64_t lgMaxK,
                  target_hll_type type1, target_hll_type type2, target_hll_type resultType) {
    uint64_t v = 0;
    //int tot = n1 + n2;

    hll_sketch h1(lgk1, type1);
    hll_sketch h2(lgk2, type2);
    int lgControlK = min(min(lgk1, lgk2), lgMaxK);
    hll_sketch control(lgControlK, resultType);

    for (uint64_t i = 0; i < n1; ++i) {
      h1.update(v + i);
      control.update(v + i);
    }
    v += n1;
    for (uint64_t i = 0; i < n2; ++i) {
      h2.update(v + i);
      control.update(v + i);
    }
    v += n2;

    hll_union u(lgMaxK);
    u.update(h1);
    u.update(h2);

    hll_sketch result = u.get_result(resultType);

    // force non-HIP estimates to avoid issues with in- vs out-of-order
    double uEst = result.get_composite_estimate();
    double uUb = result.get_upper_bound(2);
    double uLb = result.get_lower_bound(2);
    //double rerr = ((uEst/tot) - 1.0) * 100;

    double controlEst = control.get_composite_estimate();
    double controlUb = control.get_upper_bound(2);
    double controlLb = control.get_lower_bound(2);

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
        toFrom(lgK, HLL_4, n);
        toFrom(lgK, HLL_6, n);
        toFrom(lgK, HLL_8, n);
      }
    }
  }

  void checkUnionEquality(hll_union& u1, hll_union& u2) {
    hll_sketch sk1 = u1.get_result();
    hll_sketch sk2 = u2.get_result();

    CPPUNIT_ASSERT_EQUAL(sk1.get_lg_config_k(), sk2.get_lg_config_k());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.get_lower_bound(1), sk2.get_lower_bound(1), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.get_estimate(), sk2.get_estimate(), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.get_upper_bound(1), sk2.get_upper_bound(1), 0.0);
    CPPUNIT_ASSERT_EQUAL(sk1.get_target_type(), sk2.get_target_type());
  }

  void toFrom(const int lgConfigK, const target_hll_type tgtHllType, const int n) {
    hll_union srcU(lgConfigK);
    hll_sketch srcSk(lgConfigK, tgtHllType);
    for (int i = 0; i < n; ++i) {
      srcSk.update(i);
    }
    srcU.update(srcSk);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    srcU.serialize_compact(ss);
    hll_union dstU = hll_union::deserialize(ss);
    checkUnionEquality(srcU, dstU);

    auto bytes1 = srcU.serialize_compact();
    dstU = hll_union::deserialize(bytes1.data(), bytes1.size());
    checkUnionEquality(srcU, dstU);

    ss.clear();
    srcU.serialize_updatable(ss);
    dstU = hll_union::deserialize(ss);
    checkUnionEquality(srcU, dstU);

    auto bytes2 = srcU.serialize_updatable();
    dstU = hll_union::deserialize(bytes2.data(), bytes2.size());
    checkUnionEquality(srcU, dstU);
  } 

  void checkCompositeEstimate() {
    hll_union u(12);
    CPPUNIT_ASSERT(u.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, u.get_composite_estimate(), 0.03);
    for (int i = 1; i <= 15; ++i) { u.update(i); }
    CPPUNIT_ASSERT_DOUBLES_EQUAL(15.0, u.get_composite_estimate(), 15 * 0.03);
    for (int i = 16; i <= 1000; ++i) { u.update(i); }
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1000.0, u.get_composite_estimate(), 1000 * 0.03);
  }

  void checkConfigKLimits() {
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect lgK too small",
                                 hll_union(HllUtil<>::MIN_LOG_K - 1),
                                 std::invalid_argument);

    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect lgK too large",
                                 hll_union(HllUtil<>::MAX_LOG_K + 1),
                                 std::invalid_argument);
  }

  double getBound(int lgK, bool ub, bool oooFlag, int numStdDev, double est) {
    double re = RelativeErrorTables<>::getRelErr(ub, oooFlag, lgK, numStdDev);
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

  void checkConversions() {
    int lgK = 4;
    hll_sketch sk1(lgK, HLL_8);
    hll_sketch sk2(lgK, HLL_8);
    int n = 1 << 20;
    for (int i = 0; i < n; ++i) {
      sk1.update(i);
      sk2.update(i + n);
    }
    hll_union hllUnion(lgK);
    hllUnion.update(sk1);
    hllUnion.update(sk2);

    hll_sketch rsk1 = hllUnion.get_result(HLL_4);
    hll_sketch rsk2 = hllUnion.get_result(HLL_6);
    hll_sketch rsk3 = hllUnion.get_result(HLL_8);
    double est1 = rsk1.get_estimate();
    double est2 = rsk2.get_estimate();
    double est3 = rsk3.get_estimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, est2, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(est1, est3, 0.0);
  }

  // moved from UnionCaseTest in java
  void checkMisc() {
    hll_union u(12);
    int bytes = u.get_compact_serialization_bytes();
    CPPUNIT_ASSERT_EQUAL(8, bytes);
    bytes = hll_union::get_max_serialization_bytes(7);
    CPPUNIT_ASSERT_EQUAL(40 + 128, bytes);
    double v = u.get_estimate();
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, v, 0.0);
    v = u.get_lower_bound(1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, v, 0.0);
    v = u.get_upper_bound(1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, v, 0.0);
    CPPUNIT_ASSERT(u.is_empty());
    u.reset();
    CPPUNIT_ASSERT(u.is_empty());
    std::ostringstream oss(std::ios::binary);
    u.serialize_compact(oss);
    CPPUNIT_ASSERT_EQUAL(8, static_cast<int>(oss.tellp()));
  }

  void checkInputTypes() {
    hll_union u(8);

    // inserting the same value as a variety of input types
    u.update((uint8_t) 102);
    u.update((uint16_t) 102);
    u.update((uint32_t) 102);
    u.update((uint64_t) 102);
    u.update((int8_t) 102);
    u.update((int16_t) 102);
    u.update((int32_t) 102);
    u.update((int64_t) 102);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, u.get_estimate(), 0.01);

    // identical binary representations
    // no unsigned in Java, but need to sign-extend both as Java would do
    u.update((uint8_t) 255);
    u.update((int8_t) -1);

    u.update((float) -2.0);
    u.update((double) -2.0);

    std::string str = "input string";
    u.update(str);
    u.update(str.c_str(), str.length());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.0, u.get_estimate(), 0.01);

    u = hll_union(8);
    u.update((float) 0.0);
    u.update((float) -0.0);
    u.update((double) 0.0);
    u.update((double) -0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, u.get_estimate(), 0.01);

    u = hll_union(8);
    u.update(std::nanf("3"));
    u.update(std::nan("12"));
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, u.get_estimate(), 0.01);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(u.get_result().get_estimate(), u.get_estimate(), 0.01);

    u = hll_union(8);
    u.update(nullptr, 0);
    u.update("");
    CPPUNIT_ASSERT(u.is_empty());
  }
};

CPPUNIT_TEST_SUITE_REGISTRATION(HllUnionTest);

const int HllUnionTest::nArr[] = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

} /* namespace datasketches */
