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

#ifndef _COUPONLIST_INTERNAL_HPP_
#define _COUPONLIST_INTERNAL_HPP_

#include "CouponList.hpp"
#include "CubicInterpolation.hpp"
#include "HllUtil.hpp"
#include "IntArrayPairIterator.hpp"

#include <iostream>
#include <cstring>
#include <cmath>
#include <algorithm>

namespace datasketches {

template<typename A>
CouponList<A>::CouponList(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode)
  : HllSketchImpl<A>(lgConfigK, tgtHllType, curMode, false) {
    if (curMode == CurMode::LIST) {
      lgCouponArrInts = HllUtil<A>::LG_INIT_LIST_SIZE;
      oooFlag = false;
    } else { // curMode == SET
      lgCouponArrInts = HllUtil<A>::LG_INIT_SET_SIZE;
      oooFlag = true;
    }
    const int arrayLen = 1 << lgCouponArrInts;
    typedef typename std::allocator_traits<A>::template rebind_alloc<int> intAlloc;
    couponIntArr = intAlloc().allocate(arrayLen);
    std::fill(couponIntArr, couponIntArr + arrayLen, 0);
    couponCount = 0;
}

template<typename A>
CouponList<A>::CouponList(const CouponList& that)
  : HllSketchImpl<A>(that.lgConfigK, that.tgtHllType, that.curMode, false),
    lgCouponArrInts(that.lgCouponArrInts),
    couponCount(that.couponCount),
    oooFlag(that.oooFlag) {

  const int numItems = 1 << lgCouponArrInts;
  typedef typename std::allocator_traits<A>::template rebind_alloc<int> intAlloc;
  couponIntArr = intAlloc().allocate(numItems);
  std::copy(that.couponIntArr, that.couponIntArr + numItems, couponIntArr);
}

template<typename A>
CouponList<A>::CouponList(const CouponList& that, const TgtHllType tgtHllType)
  : HllSketchImpl<A>(that.lgConfigK, tgtHllType, that.curMode, false),
    lgCouponArrInts(that.lgCouponArrInts),
    couponCount(that.couponCount),
    oooFlag(that.oooFlag) {

  const int numItems = 1 << lgCouponArrInts;
  typedef typename std::allocator_traits<A>::template rebind_alloc<int> intAlloc;
  couponIntArr = intAlloc().allocate(numItems);
  std::copy(that.couponIntArr, that.couponIntArr + numItems, couponIntArr);
}

template<typename A>
CouponList<A>::~CouponList() {
  typedef typename std::allocator_traits<A>::template rebind_alloc<int> intAlloc;
  intAlloc().deallocate(couponIntArr, 1 << lgCouponArrInts);
}

template<typename A>
std::function<void(HllSketchImpl<A>*)> CouponList<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    CouponList<A>* cl = static_cast<CouponList<A>*>(ptr);
    cl->~CouponList();
    clAlloc().deallocate(cl, 1);
  };
}

template<typename A>
CouponList<A>* CouponList<A>::copy() const {
  return new (clAlloc().allocate(1)) CouponList<A>(*this);
}

template<typename A>
CouponList<A>* CouponList<A>::copyAs(const TgtHllType tgtHllType) const {
  return new (clAlloc().allocate(1)) CouponList<A>(*this, tgtHllType);
}

template<typename A>
CouponList<A>* CouponList<A>::newList(const void* bytes, size_t len) {
  if (len < HllUtil<A>::LIST_INT_ARR_START) {
    throw std::invalid_argument("Input data length insufficient to hold CouponHashSet");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::LIST_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  CurMode curMode = HllSketchImpl<A>::extractCurMode(data[HllUtil<A>::MODE_BYTE]);
  if (curMode != LIST) {
    throw std::invalid_argument("Calling set construtor with non-list mode data");
  }

  TgtHllType tgtHllType = HllSketchImpl<A>::extractTgtHllType(data[HllUtil<A>::MODE_BYTE]);

  const int lgK = data[HllUtil<A>::LG_K_BYTE];
  const bool compact = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);
  const bool oooFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool emptyFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::EMPTY_FLAG_MASK) ? true : false);

  const int couponCount = data[HllUtil<A>::LIST_COUNT_BYTE];
  const int couponsInArray = (compact ? couponCount : (1 << HllUtil<A>::computeLgArrInts(LIST, couponCount, lgK)));
  const size_t expectedLength = HllUtil<A>::LIST_INT_ARR_START + (couponsInArray * sizeof(int));
  if (len < expectedLength) {
    throw std::invalid_argument("Byte array too short for sketch. Expected " + std::to_string(expectedLength)
                                + ", found: " + std::to_string(len));
  }

  CouponList<A>* sketch = new (clAlloc().allocate(1)) CouponList<A>(lgK, tgtHllType, curMode);
  sketch->couponCount = couponCount;
  sketch->putOutOfOrderFlag(oooFlag); // should always be false for LIST

  if (!emptyFlag) {
    // only need to read valid coupons, unlike in stream case
    std::memcpy(sketch->couponIntArr, data + HllUtil<A>::LIST_INT_ARR_START, couponCount * sizeof(int));
  }
  
  return sketch;
}

template<typename A>
CouponList<A>* CouponList<A>::newList(std::istream& is) {
  uint8_t listHeader[8];
  is.read((char*)listHeader, 8 * sizeof(uint8_t));

  if (listHeader[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::LIST_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  CurMode curMode = HllSketchImpl<A>::extractCurMode(listHeader[HllUtil<A>::MODE_BYTE]);
  if (curMode != LIST) {
    throw std::invalid_argument("Calling list construtor with non-list mode data");
  }

  const TgtHllType tgtHllType = HllSketchImpl<A>::extractTgtHllType(listHeader[HllUtil<A>::MODE_BYTE]);

  const int lgK = (int) listHeader[HllUtil<A>::LG_K_BYTE];
  const bool compact = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);
  const bool oooFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool emptyFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::EMPTY_FLAG_MASK) ? true : false);

  CouponList<A>* sketch = new (clAlloc().allocate(1)) CouponList<A>(lgK, tgtHllType, curMode);
  const int couponCount = listHeader[HllUtil<A>::LIST_COUNT_BYTE];
  sketch->couponCount = couponCount;
  sketch->putOutOfOrderFlag(oooFlag); // should always be false for LIST

  if (!emptyFlag) {
    // For stream processing, need to read entire number written to stream so read
    // pointer ends up set correctly.
    // If not compact, still need to read empty items even though in order.
    const int numToRead = (compact ? couponCount : (1 << sketch->lgCouponArrInts));
    is.read((char*)sketch->couponIntArr, numToRead * sizeof(int));
  }

  return sketch;
}

template<typename A>
std::pair<std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>, const size_t> CouponList<A>::serialize(bool compact, unsigned header_size_bytes) const {
  const size_t sketchSizeBytes = (compact ? getCompactSerializationBytes() : getUpdatableSerializationBytes()) + header_size_bytes;
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint8_t> uint8Alloc;
  std::unique_ptr<uint8_t, std::function<void(uint8_t*)>> byteArr(
    uint8Alloc().allocate(sketchSizeBytes),
    [sketchSizeBytes](uint8_t* p){ uint8Alloc().deallocate(p, sketchSizeBytes); }
  );

  uint8_t* bytes = byteArr.get() + header_size_bytes;

  bytes[HllUtil<A>::PREAMBLE_INTS_BYTE] = static_cast<uint8_t>(getPreInts());
  bytes[HllUtil<A>::SER_VER_BYTE] = static_cast<uint8_t>(HllUtil<A>::SER_VER);
  bytes[HllUtil<A>::FAMILY_BYTE] = static_cast<uint8_t>(HllUtil<A>::FAMILY_ID);
  bytes[HllUtil<A>::LG_K_BYTE] = static_cast<uint8_t>(this->lgConfigK);
  bytes[HllUtil<A>::LG_ARR_BYTE] = static_cast<uint8_t>(lgCouponArrInts);
  bytes[HllUtil<A>::FLAGS_BYTE] = this->makeFlagsByte(compact);
  bytes[HllUtil<A>::LIST_COUNT_BYTE] = static_cast<uint8_t>(this->curMode == LIST ? couponCount : 0);
  bytes[HllUtil<A>::MODE_BYTE] = this->makeModeByte();

  if (this->curMode == SET) {
    std::memcpy(bytes + HllUtil<A>::HASH_SET_COUNT_INT, &couponCount, sizeof(couponCount));
  }

  // coupons
  // isCompact() is always false for now
  const int sw = (isCompact() ? 2 : 0) | (compact ? 1 : 0);
  switch (sw) {
    case 0: { // src updatable, dst updatable
      std::memcpy(bytes + getMemDataStart(), getCouponIntArr(), (1 << lgCouponArrInts) * sizeof(int));
      break;
    }
    case 1: { // src updatable, dst compact
      PairIterator_with_deleter<A> itr = getIterator();
      bytes += getMemDataStart(); // reusing ponter for incremental writes
      while (itr->nextValid()) {
        const int pairValue = itr->getPair();
        std::memcpy(bytes, &pairValue, sizeof(pairValue));
        bytes += sizeof(pairValue);
      }
      break;
    }

    default:
      throw std::runtime_error("Impossible condition when serializing");
  }

  return std::make_pair(std::move(byteArr), sketchSizeBytes);
}

template<typename A>
void CouponList<A>::serialize(std::ostream& os, const bool compact) const {
  // header
  const uint8_t preInts(getPreInts());
  os.write((char*)&preInts, sizeof(preInts));
  const uint8_t serialVersion(HllUtil<A>::SER_VER);
  os.write((char*)&serialVersion, sizeof(serialVersion));
  const uint8_t familyId(HllUtil<A>::FAMILY_ID);
  os.write((char*)&familyId, sizeof(familyId));
  const uint8_t lgKByte((uint8_t) this->lgConfigK);
  os.write((char*)&lgKByte, sizeof(lgKByte));
  const uint8_t lgArrIntsByte((uint8_t) lgCouponArrInts);
  os.write((char*)&lgArrIntsByte, sizeof(lgArrIntsByte));
  const uint8_t flagsByte(this->makeFlagsByte(compact));
  os.write((char*)&flagsByte, sizeof(flagsByte));

  if (this->curMode == LIST) {
    const uint8_t listCount((uint8_t) couponCount);
    os.write((char*)&listCount, sizeof(listCount));
  } else { // curMode == SET
    const uint8_t unused(0);
    os.write((char*)&unused, sizeof(unused));
  }

  const uint8_t modeByte(this->makeModeByte());
  os.write((char*)&modeByte, sizeof(modeByte));

  if (this->curMode == SET) {
    // writing as int, already stored as int
    os.write((char*)&couponCount, sizeof(couponCount));
  }

  // coupons
  // isCompact() is always false for now
  const int sw = (isCompact() ? 2 : 0) | (compact ? 1 : 0);
  switch (sw) {
    case 0: { // src updatable, dst updatable
      os.write((char*)getCouponIntArr(), (1 << lgCouponArrInts) * sizeof(int));
      break;
    }
    case 1: { // src updatable, dst compact
      PairIterator_with_deleter<A> itr = getIterator();
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

template<typename A>
HllSketchImpl<A>* CouponList<A>::couponUpdate(int coupon) {
  const int len = 1 << lgCouponArrInts;
  for (int i = 0; i < len; ++i) { // search for empty slot
    const int couponAtIdx = couponIntArr[i];
    if (couponAtIdx == HllUtil<A>::EMPTY) {
      couponIntArr[i] = coupon; // the actual update
      ++couponCount;
      if (couponCount >= len) { // array full
        if (this->lgConfigK < 8) {
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

template<typename A>
double CouponList<A>::getCompositeEstimate() const { return getEstimate(); }

template<typename A>
double CouponList<A>::getEstimate() const {
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount);
  return fmax(est, couponCount);
}

template<typename A>
double CouponList<A>::getLowerBound(const int numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 + (numStdDev * HllUtil<A>::COUPON_RSE));
  return fmax(tmp, couponCount);
}

template<typename A>
double CouponList<A>::getUpperBound(const int numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 - (numStdDev * HllUtil<A>::COUPON_RSE));
  return fmax(tmp, couponCount);
}

template<typename A>
bool CouponList<A>::isEmpty() const { return getCouponCount() == 0; }

template<typename A>
int CouponList<A>::getUpdatableSerializationBytes() const {
  return getMemDataStart() + (4 << getLgCouponArrInts());
}

template<typename A>
int CouponList<A>::getCouponCount() const {
  return couponCount;
}

template<typename A>
int CouponList<A>::getCompactSerializationBytes() const {
  return getMemDataStart() + (couponCount << 2);
}

template<typename A>
int CouponList<A>::getMemDataStart() const {
  return HllUtil<A>::LIST_INT_ARR_START;
}

template<typename A>
int CouponList<A>::getPreInts() const {
  return HllUtil<A>::LIST_PREINTS;
}

template<typename A>
bool CouponList<A>::isCompact() const { return false; }

template<typename A>
bool CouponList<A>::isOutOfOrderFlag() const { return oooFlag; }

template<typename A>
void CouponList<A>::putOutOfOrderFlag(bool oooFlag) {
  this->oooFlag = oooFlag;
}

template<typename A>
int CouponList<A>::getLgCouponArrInts() const {
  return lgCouponArrInts;
}

template<typename A>
int* CouponList<A>::getCouponIntArr() const {
  return couponIntArr;
}

template<typename A>
PairIterator_with_deleter<A> CouponList<A>::getIterator() const {
  typedef typename std::allocator_traits<A>::template rebind_alloc<IntArrayPairIterator<A>> iapiAlloc;
  IntArrayPairIterator<A>* itr = new (iapiAlloc().allocate(1)) IntArrayPairIterator<A>(couponIntArr, 1 << lgCouponArrInts, this->lgConfigK);
  return PairIterator_with_deleter<A>(
    itr,
    [](PairIterator<A>* ptr) {
      IntArrayPairIterator<A>* iapi = static_cast<IntArrayPairIterator<A>*>(ptr);
      iapi->~IntArrayPairIterator();
      iapiAlloc().deallocate(iapi, 1);
    }
  );
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::promoteHeapListToSet(CouponList& list) {
  return HllSketchImplFactory<A>::promoteListToSet(list);
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::promoteHeapListOrSetToHll(CouponList& src) {
  return HllSketchImplFactory<A>::promoteListOrSetToHll(src);
}

}

#endif // _COUPONLIST_INTERNAL_HPP_
