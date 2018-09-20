/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "CouponHashSet.hpp"

#include <cassert>

namespace datasketches {

static int find(const int* array, const int lgArrInts, const int coupon);

CouponHashSet::CouponHashSet(const int lgConfigK, const TgtHllType tgtHllType)
  : CouponList(lgConfigK, tgtHllType, CurMode::SET)
{
  assert(lgConfigK > 7);
}

CouponHashSet::CouponHashSet(const CouponHashSet& that)
  : CouponList(that) {}

CouponHashSet::CouponHashSet(const CouponHashSet& that, const TgtHllType tgtHllType)
  : CouponList(that, tgtHllType) {}

CouponHashSet* CouponHashSet::newSet(std::istream& is) {
  uint8_t listHeader[8];
  is.read((char*)listHeader, 8 * sizeof(uint8_t));

  if (listHeader[0] != HllUtil::HASH_SET_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[1] != HllUtil::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[2] != HllUtil::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  CurMode curMode = extractCurMode(listHeader[7]);
  if (curMode != SET) {
    throw std::invalid_argument("Calling set construtor with non-set mode data");
  }

  TgtHllType tgtHllType = extractTgtHllType(listHeader[7]);

  const int lgK = (int) listHeader[3];
  const int lgArrInts = (int) listHeader[4];
  bool compactFlag = ((listHeader[5] & HllUtil::COMPACT_FLAG_MASK) ? true : false);
  //bool oooFlag = ((listHeader[5] & HllUtil::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  //bool emptyFlag = ((listHeader[5] & HllUtil::EMPTY_FLAG_MASK) ? true : false);

  CouponHashSet* sketch = new CouponHashSet(lgK, tgtHllType);
  sketch->putOutOfOrderFlag(true);

  int couponCount;
  is.read((char*)&couponCount, sizeof(couponCount));
  // Don't set couponCount here;
  // we'll set later if updatable, and increment with updates if compact

  if (compactFlag) {
    for (int i = 0; i < couponCount; ++i) {
      int coupon;
      is.read((char*)&coupon, sizeof(coupon));
      if (coupon == HllUtil::EMPTY) { continue; }
      sketch->couponUpdate(coupon);

      /*
      HllSketchImpl* result = sketch->couponUpdate(coupon);
      if (result != sketch) {
        delete sketch;
        sketch = result;
      }
      */
    }
  } else {
    int* tmp = sketch->couponIntArr;
    sketch->lgCouponArrInts = lgArrInts;
    sketch->couponIntArr = new int[1 << lgArrInts];
    sketch->couponCount = couponCount;
    is.read((char*)sketch->couponIntArr, couponCount * sizeof(int));
    delete tmp;
  } 

  return sketch;
}

CouponHashSet* CouponHashSet::copy() const {
  return new CouponHashSet(*this);
}

CouponHashSet* CouponHashSet::copyAs(const TgtHllType tgtHllType) const {
  return new CouponHashSet(*this, tgtHllType);
}

CouponHashSet::~CouponHashSet() {}

HllSketchImpl* CouponHashSet::couponUpdate(int coupon) {
  const int index = find(couponIntArr, lgCouponArrInts, coupon);
  if (index >= 0) {
    return this; // found duplicate, ignore
  }
  couponIntArr[~index] = coupon; // found empty
  ++couponCount;
  if (checkGrowOrPromote()) {
    return promoteHeapListOrSetToHll(*this);
  }
  return this;
}

int CouponHashSet::getMemDataStart() const {
  return HllUtil::HASH_SET_INT_ARR_START;
}

int CouponHashSet::getPreInts() const {
  return HllUtil::HASH_SET_PREINTS;
}

bool CouponHashSet::checkGrowOrPromote() {
  if ((HllUtil::RESIZE_DENOM * couponCount) > (HllUtil::RESIZE_NUMER * (1 << lgCouponArrInts))) {
    if (lgCouponArrInts == (lgConfigK - 3)) { // at max size
      return true; // promote to HLL
    }
    int tgtLgCoupArrSize = lgCouponArrInts + 1;
    growHashSet(lgCouponArrInts, tgtLgCoupArrSize);
  }
  return false;
}

void CouponHashSet::growHashSet(const int srcLgCoupArrSize, const int tgtLgCoupArrSize) {
  const int tgtLen = 1 << tgtLgCoupArrSize;
  int* tgtCouponIntArr = new int[tgtLen];
  std::fill(tgtCouponIntArr, tgtCouponIntArr + tgtLen, 0);

  const int srcLen = 1 << srcLgCoupArrSize;
  for (int i = 0; i < srcLen; ++i) { // scan existing array for non-zero values
    const int fetched = couponIntArr[i];
    if (fetched != HllUtil::EMPTY) {
      const int idx = find(tgtCouponIntArr, tgtLgCoupArrSize, fetched); // search TGT array
      if (idx < 0) { // found EMPTY
        tgtCouponIntArr[~idx] = fetched; // insert
        continue;
      }
      throw new std::runtime_error("Error: Found duplicate coupon");
    }
  }

  delete couponIntArr;
  couponIntArr = tgtCouponIntArr;
  lgCouponArrInts = tgtLgCoupArrSize;
}

static int find(const int* array, const int lgArrInts, const int coupon) {
  const int arrMask = (1 << lgArrInts) - 1;
  int probe = coupon & arrMask;
  const int loopIndex = probe;
  do {
    const int couponAtIdx = array[probe];
    if (couponAtIdx == HllUtil::EMPTY) {
      return ~probe; //empty
    }
    else if (coupon == couponAtIdx) {
      return probe; //duplicate
    }
    const int stride = ((coupon & HllUtil::KEY_MASK_26) >> lgArrInts) | 1;
    probe = (probe + stride) & arrMask;
  } while (probe != loopIndex);
  throw std::invalid_argument("Key not found and no empty slots!");
}

}
