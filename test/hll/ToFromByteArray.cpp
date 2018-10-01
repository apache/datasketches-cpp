/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "src/hll/hll.hpp"
#include "src/hll/CouponList.hpp"
#include "src/hll/CouponHashSet.hpp"
#include "src/hll/HllArray.hpp"

#include "src/hll/HllSketch.hpp"
#include "src/hll/HllUnion.hpp"
#include "src/hll/HllUtil.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ostream>
#include <cmath>
#include <string>

namespace datasketches {

class ToFromByteArray : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(ToFromByteArray);
  CPPUNIT_TEST(deserializeFromJava);
  CPPUNIT_TEST_SUITE_END();

  void deserializeFromJava() {
    std::ifstream ifs;
    ifs.open("test/hll/list_from_java.bin", std::ios::binary);
    HllSketch* sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 7.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 7.0, 1e-6); // java: 7.000000104308129
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 7.000350, 1e-5); // java: 7.000349609067664

    CPPUNIT_ASSERT(((HllSketchPvt*)sk)->hllSketchImpl->getCurMode() == LIST);
    CouponList* cl = (CouponList*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(cl->getCouponCount(), 7);
    ifs.close();
    delete sk;


    ifs.open("test/hll/compact_set_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 24.001200, 1e-5); // java: 24.0011996729902

    CPPUNIT_ASSERT(((HllSketchPvt*)sk)->hllSketchImpl->getCurMode() == SET);
    CouponHashSet* chs = (CouponHashSet*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(chs->getCouponCount(), 24);
    ifs.close();
    delete sk;


    ifs.open("test/hll/updatable_set_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 24.001200, 1e-5); // java: 24.0011996729902

    CPPUNIT_ASSERT(((HllSketchPvt*)sk)->hllSketchImpl->getCurMode() == SET);
    chs = (CouponHashSet*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(chs->getCouponCount(), 24);
    ifs.close();
    delete sk;


    ifs.open("test/hll/array6_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_6);
    CPPUNIT_ASSERT(((HllSketchPvt*)sk)->hllSketchImpl->getCurMode() == HLL);
    HllArray* ha = (HllArray*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 0);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    ifs.close();
    delete sk;


    ifs.open("test/hll/compact_array4_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_4);
    CPPUNIT_ASSERT(((HllSketchPvt*)sk)->hllSketchImpl->getCurMode() == HLL);
    ha = (HllArray*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 3);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    ifs.close();
    delete sk;


    ifs.open("test/hll/updatable_array4_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_4);
    CPPUNIT_ASSERT(((HllSketchPvt*)sk)->hllSketchImpl->getCurMode() == HLL);
    ha = (HllArray*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 3);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    ifs.close();
    delete sk;

  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(ToFromByteArray);

} /* namespace datasketches */
