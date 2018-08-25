/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "HllSketchImpl.hpp"

namespace datasketches {

static int numImpls = 0;

HllSketchImpl::HllSketchImpl(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode)
  : lgConfigK(lgConfigK),
    tgtHllType(tgtHllType),
    curMode(curMode)
{ std::cerr << "Num impls: " << ++numImpls << "\n";
 }

HllSketchImpl::~HllSketchImpl() {
  std::cerr << "Num impls: " << --numImpls << "\n";
}

uint8_t HllSketchImpl::makeFlagsByte(const bool compact) const {
  uint8_t flags(0);
  flags |= (isEmpty() ? HllUtil::EMPTY_FLAG_MASK : 0);
  flags |= (compact ? HllUtil::COMPACT_FLAG_MASK : 0);
  flags |= (isOutOfOrderFlag() ? HllUtil::OUT_OF_ORDER_FLAG_MASK : 0);
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
uint8_t HllSketchImpl::makeModeByte() const {
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
  /*
  case HLL_6:
    byte |= (1 << 2);
    break;
  */
  case HLL_8:
    byte = (2 << 2); 
    break;
  }

  return byte;
}

TgtHllType HllSketchImpl::getTgtHllType() const {
  return tgtHllType;
}

int HllSketchImpl::getLgConfigK() const {
  return lgConfigK;
}

CurMode HllSketchImpl::getCurMode() const {
  return curMode;
}

}
