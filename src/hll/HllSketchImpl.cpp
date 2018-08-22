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

TgtHllType HllSketchImpl::getTgtHllType() {
  return tgtHllType;
}

int HllSketchImpl::getLgConfigK() {
  return lgConfigK;
}

CurMode HllSketchImpl::getCurMode() {
  return curMode;
}

}
