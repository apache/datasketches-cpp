/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll.hpp"
#include "HllSketch.hpp"
#include "CouponList.hpp"
#include "CouponHashSet.hpp"
#include "HllArray.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class hllSketchTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(hllSketchTest);
  CPPUNIT_TEST(checkCopies);
  CPPUNIT_TEST(checkCopyAs);
  CPPUNIT_TEST(checkMisc1);
  CPPUNIT_TEST(checkNumStdDev);
  CPPUNIT_TEST(checkSerSizes);
  CPPUNIT_TEST(checkConfigKLimits);
  CPPUNIT_TEST(exerciseToString);
  CPPUNIT_TEST(checkEmptyCoupon);
  CPPUNIT_TEST(checkCompactFlag);
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

  void checkMisc1() {
    int lgConfigK = 8;
    TgtHllType srcType = TgtHllType::HLL_8;
    HllSketch* sk = HllSketch::newInstance(lgConfigK, srcType);

    for (int i = 0; i < 7; ++i) { sk->update(i); } // LIST
    CouponList* cl = (CouponList*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(7, cl->getCouponCount());
    HllSketchImpl* impl = ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(36, impl->getCompactSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(40, impl->getUpdatableSerializationBytes());

    for (int i = 7; i < 24; ++i) { sk->update(i); } // SET
    CouponHashSet* chs = (CouponHashSet*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(24, chs->getCouponCount());
    impl = ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(108, impl->getCompactSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(140, impl->getUpdatableSerializationBytes());

    sk->update(24); // HLL
    HllArray* arr = (HllArray*) ((HllSketchPvt*)sk)->hllSketchImpl;
    CPPUNIT_ASSERT(arr->getAuxIterator() == nullptr);
    CPPUNIT_ASSERT_EQUAL(0, arr->getCurMin());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(25.0, arr->getHipAccum(), 25.0 * 0.02);
    CPPUNIT_ASSERT(arr->getNumAtCurMin() >= 0);
    CPPUNIT_ASSERT_EQUAL(40 + 256, arr->getUpdatableSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(10, arr->getPreInts());

    const int hllBytes = HllUtil::HLL_BYTE_ARR_START + (1 << lgConfigK);
    CPPUNIT_ASSERT_EQUAL(hllBytes, sk->getCompactSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(hllBytes, HllSketch::getMaxUpdatableSerializationBytes(lgConfigK, HLL_8));

    delete sk;
  }

  void checkNumStdDev() {
    try {
      HllUtil::checkNumStdDev(0);
      CPPUNIT_FAIL("Must throw: invalid number of std deviations");
    } catch (std::exception& e) {
      // expected
    }
  }

  void checkSerSizes() {
    checkSerializationSizes(8, HLL_8);
    checkSerializationSizes(8, HLL_6);
    checkSerializationSizes(8, HLL_4);
  }

  void checkSerializationSizes(const int lgConfigK, TgtHllType tgtHllType) {
    HllSketch* sk = HllSketch::newInstance(lgConfigK, tgtHllType);
    int i;

    // LIST
    for (i = 0; i < 7; ++i) { sk->update(i); }
    int expected = HllUtil::LIST_INT_ARR_START + (i << 2);
    CPPUNIT_ASSERT_EQUAL(expected, sk->getCompactSerializationBytes());
    expected = HllUtil::LIST_INT_ARR_START + (4 << HllUtil::LG_INIT_LIST_SIZE);
    CPPUNIT_ASSERT_EQUAL(expected, sk->getUpdatableSerializationBytes());

    // SET
    for (i = 7; i < 24; ++i) { sk->update(i); }
    expected = HllUtil::HASH_SET_INT_ARR_START + (i << 2);
    CPPUNIT_ASSERT_EQUAL(expected, sk->getCompactSerializationBytes());
    expected = HllUtil::HASH_SET_INT_ARR_START + (4 << HllUtil::LG_INIT_SET_SIZE);
    CPPUNIT_ASSERT_EQUAL(expected, sk->getUpdatableSerializationBytes());

    // HLL
    sk->update(i);
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, ((HllSketchPvt*) sk)->getCurrentMode());
    HllArray* hllArr = (HllArray*) ((HllSketchPvt*) sk)->hllSketchImpl;

    int auxCountBytes = 0;
    int auxArrBytes = 0;
    if (hllArr->getTgtHllType() == TgtHllType::HLL_4) {
      AuxHashMap* auxMap = hllArr->getAuxHashMap();
      if (auxMap != nullptr) {
        auxCountBytes = auxMap->getAuxCount() << 2;
        auxArrBytes = 4 << auxMap->getLgAuxArrInts();
      } else {
        auxArrBytes = 4 << HllUtil::LG_AUX_ARR_INTS[lgConfigK];
      }
    }
    int hllArrBytes = hllArr->getHllByteArrBytes();
    expected = HllUtil::HLL_BYTE_ARR_START + hllArrBytes + auxCountBytes;
    CPPUNIT_ASSERT_EQUAL(expected, sk->getCompactSerializationBytes());
    expected = HllUtil::HLL_BYTE_ARR_START + hllArrBytes + auxArrBytes;
    CPPUNIT_ASSERT_EQUAL(expected, sk->getUpdatableSerializationBytes());
    
    int fullAuxArrBytes = (tgtHllType == HLL_4) ? (4 << HllUtil::LG_AUX_ARR_INTS[lgConfigK]) : 0;
    expected = HllUtil::HLL_BYTE_ARR_START + hllArrBytes + fullAuxArrBytes;
    CPPUNIT_ASSERT_EQUAL(expected,
                         HllSketch::getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType));

    delete sk;
  }

  void checkConfigKLimits() {
    try {
      HllSketch::newInstance(HllUtil::MIN_LOG_K - 1);
      CPPUNIT_FAIL("Must fail: lgK too small");
    } catch (std::exception& e) {
      // expected
    }

    try {
      HllSketch::newInstance(HllUtil::MAX_LOG_K + 1);
      CPPUNIT_FAIL("Must fail: lgK too large");
    } catch (std::exception& e) {
      // expected
    }
  }

  void exerciseToString() {
    HllSketch* sk = HllSketch::newInstance(15, HLL_4);
    for (int i = 0; i < 25; ++i) { sk->update(i); }
    std::ostringstream oss(std::ios::binary);
    sk->to_string(oss, false, true, true, true);
    for (int i = 25; i < (1 << 20); ++i) { sk->update(i); }
    sk->to_string(oss, false, true, true, true);
    sk->to_string(oss, false, true, true, false);
    delete sk;

    sk = HllSketch::newInstance(8, HLL_8);
    for (int i = 0; i < 25; ++i) { sk->update(i); }
    sk->to_string(oss, false, true, true, true);
    delete sk;
  }
  
  void checkEmptyCoupon() {
    int lgK = 8;
    TgtHllType type = HLL_8;
    HllSketch* sk = HllSketch::newInstance(lgK, type);
    for (int i = 0; i < 20; ++i) { sk->update(i); } // SET mode
    ((HllSketchPvt*) sk)->couponUpdate(0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(20, sk->getEstimate(), 0.001);
    delete sk;
  }

  void checkCompactFlag() {
    int lgK = 8;
    // unless/until we create non-updatable "direct" versions,
    // deserialized image should never be compact
    // LIST: follows serialization request
    CPPUNIT_ASSERT(checkCompact(lgK, 7, HLL_8, false) == false);
    //CPPUNIT_ASSERT(checkCompact(lgK, 7, HLL_8, true) == true);
    CPPUNIT_ASSERT(checkCompact(lgK, 7, HLL_8, true) == false);

    // SET: follows serialization request
    CPPUNIT_ASSERT(checkCompact(lgK, 24, HLL_8, false) == false);
    //CPPUNIT_ASSERT(checkCompact(lgK, 24, HLL_8, true) == true);
    CPPUNIT_ASSERT(checkCompact(lgK, 24, HLL_8, true) == false);

    // HLL8: always updatable
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_8, false) == false);
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_8, true) == false);

    // HLL6: always updatable
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_6, false) == false);
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_6, true) == false);

    // HLL4: follows serialization request
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_4, false) == false);
    //CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_4, true) == true);
    CPPUNIT_ASSERT(checkCompact(lgK, 25, HLL_4, true) == false);
  }

  // Creates and serializes then deserializes sketch.
  // Returns true if deserialized sketch is compact.
  bool checkCompact(const int lgK, const int n, const TgtHllType type, bool compact) {
    HllSketch* sk = HllSketch::newInstance(lgK, type);
    for (int i = 0; i < n; ++i) { sk->update(i); }
    
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    if (compact) { 
      sk->serializeCompact(ss);
      CPPUNIT_ASSERT_EQUAL(sk->getCompactSerializationBytes(), (int) ss.tellp());
    } else {
      sk->serializeUpdatable(ss);
      CPPUNIT_ASSERT_EQUAL(sk->getUpdatableSerializationBytes(), (int) ss.tellp());
    }
    
    HllSketch* sk2 = HllSketch::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, sk2->getEstimate(), 0.01);
    bool isCompact = sk2->isCompact();

    delete sk;
    delete sk2;

    return isCompact;
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(hllSketchTest);

} /* namespace datasketches */
