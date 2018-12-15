/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll.hpp"
#include "CouponList.hpp"
#include "CouponHashSet.hpp"
#include "HllArray.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class ToFromByteArrayTest : public CppUnit::TestFixture {

  // list of values defined at bottom of file
  static const int nArr[]; // = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

  CPPUNIT_TEST_SUITE(ToFromByteArrayTest);
  CPPUNIT_TEST(deserializeFromJava);
  CPPUNIT_TEST(toFromSketch);
  CPPUNIT_TEST_SUITE_END();

  void deserializeFromJava() {
    std::ifstream ifs;
    ifs.open("test/list_from_java.bin", std::ios::binary);
    hll_sketch sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 7.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 7.0, 1e-6); // java: 7.000000104308129
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 7.000350, 1e-5); // java: 7.000349609067664

    HllSketchPvt* s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == LIST);
    CouponList* cl = (CouponList*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(cl->getCouponCount(), 7);
    ifs.close();


    ifs.open("test/compact_set_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 24.001200, 1e-5); // java: 24.0011996729902

    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == SET);
    CouponHashSet* chs = (CouponHashSet*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(chs->getCouponCount(), 24);
    ifs.close();


    ifs.open("test/updatable_set_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 24.001200, 1e-5); // java: 24.0011996729902

    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == SET);
    chs = (CouponHashSet*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(chs->getCouponCount(), 24);
    ifs.close();


    ifs.open("test/array6_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_6);
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == HLL);
    HllArray* ha = (HllArray*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 0);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    ifs.close();


    ifs.open("test/compact_array4_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_4);
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == HLL);
    ha = (HllArray*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 3);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    ifs.close();


    ifs.open("test/updatable_array4_from_java.bin", std::ios::binary);
    sk = HllSketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk->isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk->getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk->getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_4);
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == HLL);
    ha = (HllArray*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 3);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    ifs.close();
  }

  void toFrom(const int lgConfigK, const TgtHllType tgtHllType, const int n) {
    hll_sketch src = HllSketch::newInstance(lgConfigK, tgtHllType);
    for (int i = 0; i < n; ++i) {
      src->update(i);
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    src->serializeCompact(ss);
    hll_sketch dst = HllSketch::deserialize(ss);
    checkSketchEquality(src, dst);

    std::pair<std::unique_ptr<uint8_t>, const size_t> bytes1 = src->serializeCompact();
    dst = HllSketch::deserialize(bytes1.first.get(), bytes1.second);
    checkSketchEquality(src, dst);

    ss.clear();
    src->serializeUpdatable(ss);
    dst = HllSketch::deserialize(ss);
    checkSketchEquality(src, dst);

    std::pair<std::unique_ptr<uint8_t>, const size_t> bytes2 = src->serializeUpdatable();
    dst = HllSketch::deserialize(bytes2.first.get(), bytes2.second);
    checkSketchEquality(src, dst);
  }

  void checkSketchEquality(hll_sketch& s1, hll_sketch& s2) {
    HllSketchPvt* sk1 = static_cast<HllSketchPvt*>(s1.get());
    HllSketchPvt* sk2 = static_cast<HllSketchPvt*>(s2.get());

    CPPUNIT_ASSERT_EQUAL(sk1->getLgConfigK(), sk2->getLgConfigK());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1->getLowerBound(1), sk2->getLowerBound(1), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1->getEstimate(), sk2->getEstimate(), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1->getUpperBound(1), sk2->getUpperBound(1), 0.0);
    CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), sk2->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(sk1->getTgtHllType(), sk2->getTgtHllType());

    if (sk1->getCurrentMode() == LIST) {
      CouponList* cl1 = static_cast<CouponList*>(sk1->hllSketchImpl);
      CouponList* cl2 = static_cast<CouponList*>(sk2->hllSketchImpl);
      CPPUNIT_ASSERT_EQUAL(cl1->getCouponCount(), cl2->getCouponCount());
    } else if (sk1->getCurrentMode() == SET) {
      CouponHashSet* chs1 = static_cast<CouponHashSet*>(sk1->hllSketchImpl);
      CouponHashSet* chs2 = static_cast<CouponHashSet*>(sk2->hllSketchImpl);
      CPPUNIT_ASSERT_EQUAL(chs1->getCouponCount(), chs2->getCouponCount());
    } else { // sk1->getCurrentMode() == HLL      
      HllArray* ha1 = static_cast<HllArray*>(sk1->hllSketchImpl);
      HllArray* ha2 = static_cast<HllArray*>(sk2->hllSketchImpl);
      CPPUNIT_ASSERT_EQUAL(ha1->getCurMin(), ha2->getCurMin());
      CPPUNIT_ASSERT_EQUAL(ha1->getNumAtCurMin(), ha2->getNumAtCurMin());
      CPPUNIT_ASSERT_DOUBLES_EQUAL(ha1->getKxQ0(), ha2->getKxQ0(), 0);
      CPPUNIT_ASSERT_DOUBLES_EQUAL(ha1->getKxQ1(), ha2->getKxQ1(), 0);
    }
  }

  void toFromSketch() {
    for (int i = 0; i < 10; ++i) {
      int n = nArr[i];
      for (int lgK = 4; lgK <= 13; ++lgK) {
        toFrom(lgK, HLL_4, n);
        toFrom(lgK, HLL_6, n);
        toFrom(lgK, HLL_8, n);
      }
    }
  }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ToFromByteArrayTest);
  
const int ToFromByteArrayTest::nArr[] = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

} /* namespace datasketches */
