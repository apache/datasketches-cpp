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
//#include "HllUnion.hpp"
//#include "HllUtil.hpp"

#include <cmath>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {
/*
class UnionCaseTest : public CppUnit::TestFixture {

  static uint64_t v;
  
  CPPUNIT_TEST_SUITE(UnionCaseTest);
  CPPUNIT_TEST(checkCase0);
  CPPUNIT_TEST(checkCase1);
  CPPUNIT_TEST(checkCase2);
  CPPUNIT_TEST(checkCase2B);
  CPPUNIT_TEST(checkCase4);
  CPPUNIT_TEST(checkCase5);
  CPPUNIT_TEST(checkCase6);
  CPPUNIT_TEST(checkCase6B);
  CPPUNIT_TEST(checkCase8);
  CPPUNIT_TEST(checkCase9);
  CPPUNIT_TEST(checkCase10);
  CPPUNIT_TEST(checkCase10B);
  CPPUNIT_TEST(checkCase12);
  CPPUNIT_TEST(checkCase13);
  CPPUNIT_TEST(checkCase14);
  CPPUNIT_TEST(checkCase14B);
  CPPUNIT_TEST_SUITE_END();

  typedef HllSketch<> hll_sketch;
  typedef HllUnion<> hll_union;

  void checkCase0() { // src: LIST, gadget: LIST, cases 0, 0
    int n1 = 2;
    int n2 = 3;
    int n3 = 2;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2);
    hll_sketch h3 = buildSketch(10, HLL_8, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(false, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase1() { // src: SET, gadget: LIST, cases 0, 1
    int n1 = 5;
    int n2 = 2;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1); // LIST, 5
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2); // LIST, 2
    hll_sketch h3 = buildSketch(10, HLL_8, n3); // SET
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase2() { // src: HLL, gadget: LIST, swap, cases 0, 2
    int n1 = 5;
    int n2 = 2;
    int n3 = 97;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_8, n2);
    hll_sketch h3 = buildSketch(10, HLL_4, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(10, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(false, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase2B() { // src: HLL, gadget: LIST, swap, cases 0, 2; different lgKs
    int n1 = 5;
    int n2 = 2;
    int n3 = 769;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_8, n2);
    hll_sketch h3 = buildSketch(13, HLL_4, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(false, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase4() { // src: LIST, gadget: SET, cases 0, 4
    int n1 = 6;
    int n2 = 10;
    int n3 = 6;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2);
    hll_sketch h3 = buildSketch(10, HLL_8, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase5() { // src: SET, gadget: SET, cases 0, 5
    int n1 = 6;
    int n2 = 10;
    int n3 = 10;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2);
    hll_sketch h3 = buildSketch(10, HLL_8, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase6() { // src: HLL, gadget: SET, swap, cases 1, 6
    int n1 = 2;
    int n2 = 192;
    int n3 = 97;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_8, n2);
    hll_sketch h3 = buildSketch(10, HLL_4, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(10, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase6B() { // src: HLL, gadget: SET, swap, downsample, cases 1, 6
    int n1 = 2;
    int n2 = 20;
    int n3 = 769;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_8, n2);
    hll_sketch h3 = buildSketch(13, HLL_4, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase8() { // src: LIST, gadget: HLL, cases 2 (swap), 8
    int n1 = 6;
    int n2 = 193;
    int n3 = 7;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2);
    hll_sketch h3 = buildSketch(10, HLL_8, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(11, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(false, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase9() { // src: SET, gadget: HLL, cases 2 (swap), 9
    int n1 = 6;
    int n2 = 193;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2);
    hll_sketch h3 = buildSketch(10, HLL_8, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(11, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase10() { // src: HLL, gadget: HLL, cases 2 (swap), 10
    int n1 = 6;
    int n2 = 193;
    int n3 = 193;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2);
    hll_sketch h3 = buildSketch(10, HLL_8, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(10, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase10B() { // src: HLL, gadget: HLL, cases 2 (swap), 10, copy to HLL_8
    int n1 = 6;
    int n2 = 193;
    int n3 = 193;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1);
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2);
    hll_sketch h3 = buildSketch(11, HLL_8, n3);
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(11, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase12() { // src: LIST, gadget: empty, case 12
    int n1 = 0;
    int n2 = 0;
    int n3 = 7;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1); // LIST empty
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2); // LIST empty, ignored
    hll_sketch h3 = buildSketch(10, HLL_8, n3); // LIST
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(false, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase13() { // src: SET, gadget: empty, case 13
    int n1 = 0;
    int n2 = 0;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1); // LIST empty
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2); // LIST empty, ignored
    hll_sketch h3 = buildSketch(10, HLL_8, n3); // SET
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::SET, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(true, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase14() { // src: HLL, gadget: empty, case 14
    int n1 = 0;
    int n2 = 0;
    int n3 = 97;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1); // LIST empty
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2); // LIST empty, ignored
    hll_sketch h3 = buildSketch(10, HLL_8, n3); // LIST
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(10, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(false, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  void checkCase14B() { // src: HLL, gadget: empty, case 14, downsample
    int n1 = 0;
    int n2 = 0;
    int n3 = 395;
    int sum = n1 + n2 + n3;
    hll_union u = buildUnion(12, n1); // LIST empty
    HllUnionPvt* uPvt = static_cast<HllUnionPvt*>(u.get());
    hll_sketch h2 = buildSketch(11, HLL_6, n2); // LIST empty, ignored
    hll_sketch h3 = buildSketch(12, HLL_8, n3); // LIST
    u->update(*h2);
    CPPUNIT_ASSERT_EQUAL(CurMode::LIST, uPvt->getCurrentMode());
    u->update(*h3);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, uPvt->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(12, u->getLgConfigK());
    CPPUNIT_ASSERT_EQUAL(false, uPvt->isOutOfOrderFlag());
    double err = sum * errorFactor(uPvt->getLgConfigK(), uPvt->isOutOfOrderFlag(), 2.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sum, u->getEstimate(), err);
  }

  double errorFactor(int lgK, bool oooFlag, double numStdDev) {
    if (oooFlag) {
      return (1.2 * numStdDev) / sqrt(1 << lgK);
    } else {
      return (0.9 * numStdDev) / sqrt(1 << lgK);
    }
  }

  hll_union buildUnion(int lgMaxK, int n) {
    hll_union u(lgMaxK);
    for (int i = 0; i < n; ++i) { u.update(i + v); }
    v += n;
    return std::move(u);
  }

  hll_sketch buildSketch(int lgK, TgtHllType type, int n) {
    hll_sketch sk(lgK);
    for (int i = 0; i < n; ++i) { sk.update(i + v); }
    v += n;
    return std::move(sk);
  }

};

uint64_t UnionCaseTest::v = 0;

CPPUNIT_TEST_SUITE_REGISTRATION(UnionCaseTest);
*/
} /* namespace datasketches */
