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

uint8_t HllSketchImpl::makeFlagsByte() const {
  uint8_t flags(0);
  flags |= (isEmpty() ? HllUtil::EMPTY_FLAG_MASK : 0);
  flags |= (isCompact() ? HllUtil::COMPACT_FLAG_MASK : 0);
  flags |= (isOutOfOrderFlag() ? HllUtil::OUT_OF_ORDER_FLAG_MASK : 0);
  return flags;
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
