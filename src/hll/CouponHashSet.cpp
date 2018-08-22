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

CouponHashSet* CouponHashSet::copy() {
  return new CouponHashSet(*this);
}

CouponHashSet* CouponHashSet::copyAs(const TgtHllType tgtHllType) {
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

int CouponHashSet::getMemDataStart() {
  return HllUtil::HASH_SET_INT_ARR_START;
}

int CouponHashSet::getPreInts() {
  return HllUtil::HASH_SET_PREINTS;
}

bool CouponHashSet::checkGrowOrPromote() {
  if ((HllUtil::RESIZE_DENOM * couponCount) > (HllUtil::RESIZE_NUMER * (1 << lgCouponArrInts))) {
    if (lgCouponArrInts == (lgConfigK - 3)) { // at max size
      return true; // promote to HLL
    }
    growHashSet(++lgCouponArrInts);
  }
  return false;
}

void CouponHashSet::growHashSet(const int tgtLgCoupArrSize) {
  const int tgtLen = 1 << tgtLgCoupArrSize;
  int* tgtCouponIntArr = new int[tgtLen];
  std::fill(tgtCouponIntArr, tgtCouponIntArr + tgtLen, 0);

  for (int i = 0; i < tgtLen; ++i) { // scan existing array for non-zero values
    const int fetched = couponIntArr[i];
    if (fetched != HllUtil::EMPTY) {
      const int idx = find(tgtCouponIntArr, tgtLen, fetched); // search TGT array
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
