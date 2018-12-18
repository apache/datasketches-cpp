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
  CPPUNIT_TEST(checkKLimits);
  CPPUNIT_TEST(checkInputTypes);
  CPPUNIT_TEST_SUITE_END();

  void checkCopies() {
    runCheckCopy(14, HLL_4);
    runCheckCopy(8, HLL_6);
    runCheckCopy(8, HLL_8);
  }

  void runCheckCopy(int lgConfigK, TgtHllType tgtHllType) {
    hll_sketch skContainer = HllSketch::newInstance(lgConfigK, tgtHllType);
    HllSketchPvt* sk = static_cast<HllSketchPvt*>(skContainer.get());

    for (int i = 0; i < 7; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::LIST);

    hll_sketch skCopy = sk->copy();
    HllSketchPvt* skCopyPvt = static_cast<HllSketchPvt*>(skCopy.get());
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

    skCopy = sk->copy();
    skCopyPvt = static_cast<HllSketchPvt*>(skCopy.get());
    CPPUNIT_ASSERT_EQUAL(skCopyPvt->getCurrentMode(), CurMode::SET);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    int u = (sk->getTgtHllType() == HLL_4) ? 100000 : 25;
    for (int i = 24; i < u; ++i) {
      sk->update(i);
    }
    CPPUNIT_ASSERT_EQUAL(sk->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT(sk->getCurrentMode() != skCopyPvt->getCurrentMode());
    CPPUNIT_ASSERT(sk->getEstimate() != skCopy->getEstimate()); // either 1 or 100k difference

    skCopy = sk->copy();
    skCopyPvt = static_cast<HllSketchPvt*>(skCopy.get());
    CPPUNIT_ASSERT_EQUAL(skCopyPvt->getCurrentMode(), CurMode::HLL);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(skCopy->getEstimate(), sk->getEstimate(), 0.0);

    CPPUNIT_ASSERT(sk->hllSketchImpl != skCopyPvt->hllSketchImpl);
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

    hll_sketch src = HllSketch::newInstance(lgK, srcType);
    for (int i = 0; i < n1; ++i) {
      src->update(i + base);
    }
    hll_sketch dst = src->copyAs(dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst->getEstimate(), src->getEstimate(), 0.0);

    for (int i = n1; i < n2; ++i) {
      src->update(i + base);
    }
    dst = src->copyAs(dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst->getEstimate(), src->getEstimate(), 0.0);

    for (int i = n2; i < n3; ++i) {
      src->update(i + base);
    }
    dst = src->copyAs(dstType);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(dst->getEstimate(), src->getEstimate(), 0.0);
  }

  void checkMisc1() {
    int lgConfigK = 8;
    TgtHllType srcType = TgtHllType::HLL_8;
    hll_sketch sk = HllSketch::newInstance(lgConfigK, srcType);
    HllSketchPvt* s = static_cast<HllSketchPvt*>(sk.get());

    for (int i = 0; i < 7; ++i) { sk->update(i); } // LIST
    CouponList* cl = (CouponList*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(7, cl->getCouponCount());
    HllSketchImpl* impl = s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(36, impl->getCompactSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(40, impl->getUpdatableSerializationBytes());

    for (int i = 7; i < 24; ++i) { sk->update(i); } // SET
    CouponHashSet* chs = (CouponHashSet*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(24, chs->getCouponCount());
    impl = s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(108, impl->getCompactSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(140, impl->getUpdatableSerializationBytes());

    sk->update(24); // HLL
    HllArray* arr = (HllArray*) s->hllSketchImpl;
    CPPUNIT_ASSERT(arr->getAuxIterator() == nullptr);
    CPPUNIT_ASSERT_EQUAL(0, arr->getCurMin());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(25.0, arr->getHipAccum(), 25.0 * 0.02);
    CPPUNIT_ASSERT(arr->getNumAtCurMin() >= 0);
    CPPUNIT_ASSERT_EQUAL(40 + 256, arr->getUpdatableSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(10, arr->getPreInts());

    const int hllBytes = HllUtil::HLL_BYTE_ARR_START + (1 << lgConfigK);
    CPPUNIT_ASSERT_EQUAL(hllBytes, sk->getCompactSerializationBytes());
    CPPUNIT_ASSERT_EQUAL(hllBytes, HllSketch::getMaxUpdatableSerializationBytes(lgConfigK, HLL_8));
  }

  void checkNumStdDev() {
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw on invalid number of std deviations",
                                 HllUtil::checkNumStdDev(0), std::invalid_argument);
  }

  void checkSerSizes() {
    checkSerializationSizes(8, HLL_8);
    checkSerializationSizes(8, HLL_6);
    checkSerializationSizes(8, HLL_4);
  }

  void checkSerializationSizes(const int lgConfigK, TgtHllType tgtHllType) {
    hll_sketch sk = HllSketch::newInstance(lgConfigK, tgtHllType);
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
    HllSketchPvt* s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT_EQUAL(CurMode::HLL, s->getCurrentMode());
    HllArray* hllArr = (HllArray*) s->hllSketchImpl;

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
  }

  void checkConfigKLimits() {
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw with lgK too small",
                                 HllSketch::newInstance(HllUtil::MIN_LOG_K - 1),
                                 std::invalid_argument);

    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw with lgK too large",
                                 HllSketch::newInstance(HllUtil::MAX_LOG_K + 1),
                                 std::invalid_argument);
  }

  void exerciseToString() {
    hll_sketch sk = HllSketch::newInstance(15, HLL_4);
    for (int i = 0; i < 25; ++i) { sk->update(i); }
    std::ostringstream oss(std::ios::binary);
    sk->to_string(oss, false, true, true, true);
    for (int i = 25; i < (1 << 20); ++i) { sk->update(i); }
    sk->to_string(oss, false, true, true, true);
    sk->to_string(oss, false, true, true, false);

    sk = HllSketch::newInstance(8, HLL_8);
    for (int i = 0; i < 25; ++i) { sk->update(i); }
    sk->to_string(oss, false, true, true, true);
  }
  
  void checkEmptyCoupon() {
    int lgK = 8;
    TgtHllType type = HLL_8;
    hll_sketch sk = HllSketch::newInstance(lgK, type);
    for (int i = 0; i < 20; ++i) { sk->update(i); } // SET mode
    static_cast<HllSketchPvt*>(sk.get())->couponUpdate(0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(20, sk->getEstimate(), 0.001);
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
    hll_sketch sk = HllSketch::newInstance(lgK, type);
    for (int i = 0; i < n; ++i) { sk->update(i); }
    
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    if (compact) { 
      sk->serializeCompact(ss);
      CPPUNIT_ASSERT_EQUAL(sk->getCompactSerializationBytes(), (int) ss.tellp());
    } else {
      sk->serializeUpdatable(ss);
      CPPUNIT_ASSERT_EQUAL(sk->getUpdatableSerializationBytes(), (int) ss.tellp());
    }
    
    hll_sketch sk2 = HllSketch::deserialize(ss);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, sk2->getEstimate(), 0.01);
    bool isCompact = sk2->isCompact();

    return isCompact;
  }

  void checkKLimits() {
    hll_sketch sketch1 = HllSketch::newInstance(HllUtil::MIN_LOG_K, TgtHllType::HLL_8);
    hll_sketch sketch2 = HllSketch::newInstance(HllUtil::MAX_LOG_K, TgtHllType::HLL_4);
    CPPUNIT_ASSERT_THROW(HllSketch::newInstance(HllUtil::MIN_LOG_K - 1, TgtHllType::HLL_4), std::invalid_argument);
    CPPUNIT_ASSERT_THROW(HllSketch::newInstance(HllUtil::MAX_LOG_K + 1, TgtHllType::HLL_8), std::invalid_argument);
  }

    void checkInputTypes() {
    hll_sketch sk = HllSketch::newInstance(8, TgtHllType::HLL_8);

    // inserting the same value as a variety of input types
    sk->update((uint8_t) 102);
    sk->update((uint16_t) 102);
    sk->update((uint32_t) 102);
    sk->update((uint64_t) 102);
    sk->update((int8_t) 102);
    sk->update((int16_t) 102);
    sk->update((int32_t) 102);
    sk->update((int64_t) 102);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, sk->getEstimate(), 0.01);

    // identical binary representations
    // no unsigned in Java, but need to sign-extend both as Java would do 
    sk->update((uint8_t) 255);
    sk->update((int8_t) -1);

    sk->update((float) -2.0);
    sk->update((double) -2.0);

    std::string str = "input string";
    sk->update(str);
    sk->update(str.c_str(), str.length());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.0, sk->getEstimate(), 0.01);

    sk = HllSketch::newInstance(8, TgtHllType::HLL_6);
    sk->update((float) 0.0);
    sk->update((float) -0.0);
    sk->update((double) 0.0);
    sk->update((double) -0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, sk->getEstimate(), 0.01);

    sk = HllSketch::newInstance(8, TgtHllType::HLL_4);
    sk->update(std::nanf("3"));
    sk->update(std::nan("9"));
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, sk->getEstimate(), 0.01);

    sk = HllSketch::newInstance(8, TgtHllType::HLL_4);
    sk->update(nullptr, 0);
    sk->update("");
    CPPUNIT_ASSERT(sk->isEmpty());
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(hllSketchTest);

} /* namespace datasketches */
