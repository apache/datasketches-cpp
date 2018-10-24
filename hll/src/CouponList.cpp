/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "CouponList.hpp"
#include "CouponHashSet.hpp"
#include "CubicInterpolation.hpp"
#include "HllUtil.hpp"
#include "IntArrayPairIterator.hpp"
#include "HllArray.hpp"

#include <iostream>
#include <cstring>
#include <cmath>
#include <algorithm>

namespace datasketches {

CouponList::CouponList(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode)
  : HllSketchImpl(lgConfigK, tgtHllType, curMode) {
    if (curMode == CurMode::LIST) {
      lgCouponArrInts = HllUtil::LG_INIT_LIST_SIZE;
      oooFlag = false;
    } else { // curMode == SET
      lgCouponArrInts = HllUtil::LG_INIT_SET_SIZE;
      oooFlag = true;
    }
    const int arrayLen = 1 << lgCouponArrInts;
    couponIntArr = new int[arrayLen];
    std::fill(couponIntArr, couponIntArr + arrayLen, 0);
    couponCount = 0;
}

CouponList::CouponList(const CouponList& that)
  : HllSketchImpl(that.lgConfigK, that.tgtHllType, that.curMode),
    lgCouponArrInts(that.lgCouponArrInts),
    couponCount(that.couponCount),
    oooFlag(that.oooFlag) {

  const int numItems = 1 << lgCouponArrInts;
  couponIntArr = new int[numItems];
  std::copy(that.couponIntArr, that.couponIntArr + numItems, couponIntArr);
}

CouponList::CouponList(const CouponList& that, const TgtHllType tgtHllType)
  : HllSketchImpl(that.lgConfigK, tgtHllType, that.curMode),
    lgCouponArrInts(that.lgCouponArrInts),
    couponCount(that.couponCount),
    oooFlag(that.oooFlag) {

  const int numItems = 1 << lgCouponArrInts;
  couponIntArr = new int[numItems];
  std::copy(that.couponIntArr, that.couponIntArr + numItems, couponIntArr);
}

CouponList::~CouponList() {
  delete couponIntArr;
}

CouponList* CouponList::copy() const {
  return new CouponList(*this);
}

CouponList* CouponList::copyAs(const TgtHllType tgtHllType) const {
  return new CouponList(*this, tgtHllType);
}

CouponList* CouponList::newList(std::istream& is) {
  uint8_t listHeader[8];
  is.read((char*)listHeader, 8 * sizeof(uint8_t));

  if (listHeader[0] != HllUtil::LIST_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[1] != HllUtil::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[2] != HllUtil::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  CurMode curMode = extractCurMode(listHeader[7]);
  if (curMode != LIST) {
    throw std::invalid_argument("Calling list construtor with non-list mode data");
  }

  TgtHllType tgtHllType = extractTgtHllType(listHeader[7]);

  const int lgK = (int) listHeader[3];
  //const int lgArrInts = (int) listHeader[4];
  bool compact = ((listHeader[5] & HllUtil::COMPACT_FLAG_MASK) ? true : false);
  bool oooFlag = ((listHeader[5] & HllUtil::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  bool emptyFlag = ((listHeader[5] & HllUtil::EMPTY_FLAG_MASK) ? true : false);

  CouponList* sketch = new CouponList(lgK, tgtHllType, curMode);
  const int couponCount = (int) listHeader[6];
  sketch->couponCount = couponCount;
  sketch->putOutOfOrderFlag(oooFlag); // should always be false for LIST

  if (!emptyFlag) {
    // For stream processing, need to read entire number written to stream so read
    // pointer ends up set correctly.
    // If not compact, still need to read empty items even though in order.
    int numToRead = (compact ? couponCount : (1 << sketch->lgCouponArrInts));
    is.read((char*)sketch->couponIntArr, numToRead * sizeof(int));
  }
  
  return sketch;
}

void CouponList::serialize(std::ostream& os, const bool compact) const {
  // header
  const uint8_t preInts(getPreInts());
  os.write((char*)&preInts, sizeof(preInts));
  const uint8_t serialVersion(HllUtil::SER_VER);
  os.write((char*)&serialVersion, sizeof(serialVersion));
  const uint8_t familyId(HllUtil::FAMILY_ID);
  os.write((char*)&familyId, sizeof(familyId));
  const uint8_t lgKByte((uint8_t) lgConfigK);
  os.write((char*)&lgKByte, sizeof(lgKByte));
  const uint8_t lgArrIntsByte((uint8_t) lgCouponArrInts);
  os.write((char*)&lgArrIntsByte, sizeof(lgArrIntsByte));
  const uint8_t flagsByte(makeFlagsByte(compact));
  os.write((char*)&flagsByte, sizeof(flagsByte));

  if (curMode == LIST) {
    const uint8_t listCount((uint8_t) couponCount);
    os.write((char*)&listCount, sizeof(listCount));
  } else { // curMode == SET
    const uint8_t unused(0);
    os.write((char*)&unused, sizeof(unused));
  }

  const uint8_t modeByte(makeModeByte());
  os.write((char*)&modeByte, sizeof(modeByte));

  if (curMode == SET) {
    // writing as int, already stored as int
    os.write((char*)&couponCount, sizeof(couponCount));
  }

  // coupons
  // isCompact() is always false for now
  const int sw = (isCompact() ? 2 : 0) | (compact ? 1 : 0);
  switch (sw) {
    case 0: { // src updatable, dst updatable
      os.write((char*)couponIntArr, (1 << lgCouponArrInts) * sizeof(int));
      break;
    }
    case 1: { // src updatable, dst compact
      std::unique_ptr<PairIterator> itr = getIterator();
      while (itr->nextValid()) {
        const int pairValue = itr->getPair();
        os.write((char*)&pairValue, sizeof(pairValue));
      }
      break;
    }

    default:
      throw std::runtime_error("Impossible condition when serializing");
  }
  
  return;
}

HllSketchImpl* CouponList::couponUpdate(int coupon) {
  const int len = 1 << lgCouponArrInts;
  for (int i = 0; i < len; ++i) { // search for empty slot
    const int couponAtIdx = couponIntArr[i];
    if (couponAtIdx == HllUtil::EMPTY) {
      couponIntArr[i] = coupon; // the actual update
      ++couponCount;
      if (couponCount >= len) { // array full
        if (lgConfigK < 8) {
          return promoteHeapListOrSetToHll(*this); // oooFlag = false
        }
        return promoteHeapListToSet(*this); // oooFlag = true;
      }
      return this;
    }
    // cell not empty
    if (couponAtIdx == coupon) {
      return this; // duplicate
    }
    // cell not empty and not a duplicate, continue
  }
  throw std::runtime_error("Array invalid: no empties and no duplicates");
}

double CouponList::getCompositeEstimate() const { return getEstimate(); }

double CouponList::getEstimate() const {
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation::usingXAndYTables(couponCount);
  return fmax(est, couponCount);
}

double CouponList::getLowerBound(const int numStdDev) const {
  HllUtil::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 + (numStdDev * HllUtil::COUPON_RSE));
  return fmax(tmp, couponCount);
}

double CouponList::getUpperBound(const int numStdDev) const {
  HllUtil::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 - (numStdDev * HllUtil::COUPON_RSE));
  return fmax(tmp, couponCount);
}

bool CouponList::isEmpty() const { return getCouponCount() == 0; }

int CouponList::getUpdatableSerializationBytes() const {
  return getMemDataStart() + (4 << getLgCouponArrInts());
}

int CouponList::getCouponCount() const {
  return couponCount;
}

int CouponList::getCompactSerializationBytes() const {
  return getMemDataStart() + (couponCount << 2);
}

int CouponList::getMemDataStart() const {
  return HllUtil::LIST_INT_ARR_START;
}

int CouponList::getPreInts() const {
  return HllUtil::LIST_PREINTS;
}

bool CouponList::isCompact() const { return false; }

bool CouponList::isOutOfOrderFlag() const { return oooFlag; }

void CouponList::putOutOfOrderFlag(bool oooFlag) {
  this->oooFlag = oooFlag;
}

CouponList* CouponList::reset() {
  return new CouponList(lgConfigK, tgtHllType, CurMode::LIST);
}

int CouponList::getLgCouponArrInts() const {
  return lgCouponArrInts;
}

int* CouponList::getCouponIntArr() {
  return couponIntArr;
}

std::unique_ptr<PairIterator> CouponList::getIterator() const {
  PairIterator* itr = new IntArrayPairIterator(couponIntArr, 1 << lgCouponArrInts, lgConfigK);
  return std::unique_ptr<PairIterator>(itr);
}

HllSketchImpl* CouponList::promoteHeapListToSet(CouponList& list) {
  const int couponCount = list.couponCount;
  const int* arr = list.couponIntArr;
  CouponHashSet* chSet = new CouponHashSet(list.lgConfigK, list.tgtHllType);
  for (int i = 0; i < couponCount; ++i) {
    chSet->couponUpdate(arr[i]);
  }
  chSet->putOutOfOrderFlag(true);

  return chSet;
}

HllSketchImpl* CouponList::promoteHeapListOrSetToHll(CouponList& src) {
  HllArray* tgtHllArr = HllArray::newHll(src.lgConfigK, src.tgtHllType);
  std::unique_ptr<PairIterator> srcItr = src.getIterator();
  tgtHllArr->putKxQ0(1 << src.lgConfigK);
  while (srcItr->nextValid()) {
    tgtHllArr->couponUpdate(srcItr->getPair());
    tgtHllArr->putHipAccum(src.getEstimate());
  }
  tgtHllArr->putOutOfOrderFlag(false);
  return tgtHllArr;
}

}
