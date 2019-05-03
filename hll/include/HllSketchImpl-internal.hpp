/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLSKETCHIMPL_INTERNAL_HPP_
#define _HLLSKETCHIMPL_INTERNAL_HPP_

#include "HllSketchImpl.hpp"
#include "HllSketchImplFactory.hpp"
//#include "CouponList.hpp"
//#include "CouponHashSet.hpp"
//#include "HllArray.hpp"

namespace datasketches {

#ifdef DEBUG
static int numImpls = 0;
#endif

template<typename A>
HllSketchImpl<A>::HllSketchImpl(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode)
  : lgConfigK(lgConfigK),
    tgtHllType(tgtHllType),
    curMode(curMode)
{
#ifdef DEBUG
  std::cerr << "Num impls: " << ++numImpls << "\n";
#endif
}

template<typename A>
HllSketchImpl<A>::~HllSketchImpl() {
#ifdef DEBUG
  std::cerr << "Num impls: " << --numImpls << "\n";
#endif
}

/*
template<typename A>
HllSketchImpl* HllSketchImpl<A>::deserialize(std::istream& is) {
  // we'll hand off the sketch based on PreInts so we don't need
  // to move the stream pointer back and forth -- perhaps somewhat fragile?
  const int preInts = is.peek();
  if (preInts == HllUtil<A>::HLL_PREINTS) {
    return HllArray::newHll(is);
  } else if (preInts == HllUtil<A>::HASH_SET_PREINTS) {
    return CouponHashSet::newSet(is);
  } else if (preInts == HllUtil<A>::LIST_PREINTS) {
    return CouponList::newList(is);
  } else {
    throw std::invalid_argument("Attempt to deserialize unknown object type");
  }
}

template<typename A>
HllSketchImpl* HllSketchImpl<A>::deserialize(const void* bytes, size_t len) {
  // read current mode directly
  const int preInts = static_cast<const uint8_t*>(bytes)[0];
  if (preInts == HllUtil<A>::HLL_PREINTS) {
    return HllArray::newHll(bytes, len);
  } else if (preInts == HllUtil<A>::HASH_SET_PREINTS) {
    return CouponHashSet::newSet(bytes, len);
  } else if (preInts == HllUtil<A>::LIST_PREINTS) {
    return CouponList::newList(bytes, len);
  } else {
    throw std::invalid_argument("Attempt to deserialize unknown object type");
  }
}
*/

template<typename A>
TgtHllType HllSketchImpl<A>::extractTgtHllType(const uint8_t modeByte) {
  switch ((modeByte >> 2) & 0x3) {
  case 0:
    return TgtHllType::HLL_4;
  case 1:
    return TgtHllType::HLL_6;
  case 2:
    return TgtHllType::HLL_8;
  default:
    throw std::invalid_argument("Invalid current sketch mode");
  }
}

template<typename A>
CurMode HllSketchImpl<A>::extractCurMode(const uint8_t modeByte) {
  switch (modeByte & 0x3) {
  case 0:
    return CurMode::LIST;
  case 1:
    return CurMode::SET;
  case 2:
    return CurMode::HLL;
  default:
    throw std::invalid_argument("Invalid current sketch mode");
  }
}

template<typename A>
uint8_t HllSketchImpl<A>::makeFlagsByte(const bool compact) const {
  uint8_t flags(0);
  flags |= (isEmpty() ? HllUtil<A>::EMPTY_FLAG_MASK : 0);
  flags |= (compact ? HllUtil<A>::COMPACT_FLAG_MASK : 0);
  flags |= (isOutOfOrderFlag() ? HllUtil<A>::OUT_OF_ORDER_FLAG_MASK : 0);
  return flags;
}

// lo2bits = curMode, next 2 bits = tgtHllType
// Dec  Lo4Bits TgtHllType, CurMode
//   0     0000      HLL_4,    LIST
//   1     0001      HLL_4,     SET
//   2     0010      HLL_4,     HLL
//   4     0100      HLL_6,    LIST
//   5     0101      HLL_6,     SET
//   6     0110      HLL_6,     HLL
//   8     1000      HLL_8,    LIST
//   9     1001      HLL_8,     SET
//  10     1010      HLL_8,     HLL
template<typename A>
uint8_t HllSketchImpl<A>::makeModeByte() const {
  uint8_t byte;

  switch (curMode) {
  case LIST:
    byte = 0;
    break;
  case SET:
    byte = 1;
    break;
  case HLL:
    byte = 2;
    break;
  }

  switch (tgtHllType) {
  case HLL_4:
    byte |= (0 << 2);  // for completeness
    break;
  case HLL_6:
    byte |= (1 << 2);
    break;
  case HLL_8:
    byte |= (2 << 2); 
    break;
  }

  return byte;
}

template<typename A>
HllSketchImpl<A>* HllSketchImpl<A>::reset() {
  return HllSketchImplFactory<A>::reset(this);
}

template<typename A>
TgtHllType HllSketchImpl<A>::getTgtHllType() const {
  return tgtHllType;
}

template<typename A>
int HllSketchImpl<A>::getLgConfigK() const {
  return lgConfigK;
}

template<typename A>
CurMode HllSketchImpl<A>::getCurMode() const {
  return curMode;
}

}

#endif // _HLLSKETCHIMPL_INTERNAL_HPP_
