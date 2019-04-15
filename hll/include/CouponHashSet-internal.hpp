/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _COUPONHASHSET_INTERNAL_HPP_
#define _COUPONHASHSET_INTERNAL_HPP_

#include "CouponHashSet.hpp"

#include <string>
#include <cstring>
#include <exception>

namespace datasketches {

static int find(const int* array, const int lgArrInts, const int coupon);

CouponHashSet::CouponHashSet(const int lgConfigK, const TgtHllType tgtHllType)
  : CouponList(lgConfigK, tgtHllType, CurMode::SET)
{
  if (lgConfigK <= 7) {
    throw std::invalid_argument("CouponHashSet must be initialized iwth lgConfigK > 7. Found: "
                                + std::to_string(lgConfigK));
  }
    
}

CouponHashSet::CouponHashSet(const CouponHashSet& that)
  : CouponList(that) {}

CouponHashSet::CouponHashSet(const CouponHashSet& that, const TgtHllType tgtHllType)
  : CouponList(that, tgtHllType) {}

CouponHashSet* CouponHashSet::newSet(const void* bytes, size_t len) {
  if (len < HllUtil::HASH_SET_INT_ARR_START) { // hard-coded 
    throw std::invalid_argument("Input data length insufficient to hold CouponHashSet");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[HllUtil::PREAMBLE_INTS_BYTE] != HllUtil::HASH_SET_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[HllUtil::SER_VER_BYTE] != HllUtil::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[HllUtil::FAMILY_BYTE] != HllUtil::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  CurMode curMode = extractCurMode(data[HllUtil::MODE_BYTE]);
  if (curMode != SET) {
    throw std::invalid_argument("Calling set construtor with non-set mode data");
  }

  TgtHllType tgtHllType = extractTgtHllType(data[HllUtil::MODE_BYTE]);

  const int lgK = (int) data[HllUtil::LG_K_BYTE];
  int lgArrInts = (int) data[HllUtil::LG_ARR_BYTE];
  bool compactFlag = ((data[HllUtil::FLAGS_BYTE] & HllUtil::COMPACT_FLAG_MASK) ? true : false);

  CouponHashSet* sketch = new CouponHashSet(lgK, tgtHllType);
  sketch->putOutOfOrderFlag(true);

  int couponCount;
  std::memcpy(&couponCount, data + HllUtil::HASH_SET_COUNT_INT, sizeof(couponCount));
  if (lgArrInts < HllUtil::LG_INIT_SET_SIZE) { 
    lgArrInts = HllUtil::computeLgArrInts(SET, couponCount, lgK);
  }
  // Don't set couponCount in sketch here;
  // we'll set later if updatable, and increment with updates if compact

  int couponsInArray = (compactFlag ? couponCount : (1 << sketch->getLgCouponArrInts()));
  int expectedLength = HllUtil::HASH_SET_INT_ARR_START + (couponsInArray * sizeof(int));
  if (len < expectedLength) {
    throw std::invalid_argument("Byte array too short for sketch. Expected " + std::to_string(expectedLength)
                                + ", found: " + std::to_string(len));
  }

  if (compactFlag) {
    const uint8_t* curPos = data + HllUtil::HASH_SET_INT_ARR_START;
    int coupon;
    for (int i = 0; i < couponCount; ++i, curPos += sizeof(coupon)) {
      std::memcpy(&coupon, curPos, sizeof(coupon));
      sketch->couponUpdate(coupon);
    }
  } else {
    int* tmp = sketch->couponIntArr;
    sketch->lgCouponArrInts = lgArrInts;
    sketch->couponIntArr = new int[1 << lgArrInts];
    sketch->couponCount = couponCount;
    // only need to read valid coupons, unlike in stream case
    std::memcpy(sketch->couponIntArr,
                data + HllUtil::HASH_SET_INT_ARR_START,
                couponCount * sizeof(int));
    delete tmp;
  }

  return sketch;
}

CouponHashSet* CouponHashSet::newSet(std::istream& is) {
  uint8_t listHeader[8];
  is.read((char*)listHeader, 8 * sizeof(uint8_t));

  if (listHeader[HllUtil::PREAMBLE_INTS_BYTE] != HllUtil::HASH_SET_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[HllUtil::SER_VER_BYTE] != HllUtil::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[HllUtil::FAMILY_BYTE] != HllUtil::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  CurMode curMode = extractCurMode(listHeader[HllUtil::MODE_BYTE]);
  if (curMode != SET) {
    throw std::invalid_argument("Calling set construtor with non-set mode data");
  }

  TgtHllType tgtHllType = extractTgtHllType(listHeader[HllUtil::MODE_BYTE]);

  const int lgK = (int) listHeader[HllUtil::LG_K_BYTE];
  int lgArrInts = (int) listHeader[HllUtil::LG_ARR_BYTE];
  bool compactFlag = ((listHeader[HllUtil::FLAGS_BYTE] & HllUtil::COMPACT_FLAG_MASK) ? true : false);
  //bool oooFlag = ((listHeader[HllUtil::FLAGS_BYTE] & HllUtil::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  //bool emptyFlag = ((listHeader[HllUtil::FLAGS_BYTE] & HllUtil::EMPTY_FLAG_MASK) ? true : false);

  CouponHashSet* sketch = new CouponHashSet(lgK, tgtHllType);
  sketch->putOutOfOrderFlag(true);

  int couponCount;
  is.read((char*)&couponCount, sizeof(couponCount));
  if (lgArrInts < HllUtil::LG_INIT_SET_SIZE) { 
    lgArrInts = HllUtil::computeLgArrInts(SET, couponCount, lgK);
  }
  // Don't set couponCount here;
  // we'll set later if updatable, and increment with updates if compact

  if (compactFlag) {
    for (int i = 0; i < couponCount; ++i) {
      int coupon;
      is.read((char*)&coupon, sizeof(coupon));
      sketch->couponUpdate(coupon);
    }
  } else {
    int* tmp = sketch->couponIntArr;
    sketch->lgCouponArrInts = lgArrInts;
    sketch->couponIntArr = new int[1 << lgArrInts];
    sketch->couponCount = couponCount;
    // for stream processing, read entire list so read pointer ends up set correctly
    //is.read((char*)sketch->couponIntArr, couponCount * sizeof(int));
    is.read((char*)sketch->couponIntArr, (1 << sketch->lgCouponArrInts) * sizeof(int));
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

#endif // _COUPONHASHSET_INTERNAL_HPP_
