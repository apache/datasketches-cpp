/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "HllSketch.hpp"
#include "HllUtil.hpp"
#include "CouponList.hpp"
#include "HllArray.hpp"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>

namespace datasketches {

HllSketch::HllSketch(const int lgConfigK, const TgtHllType tgtHllType) {
  hllSketchImpl = new CouponList(HllUtil::checkLgK(lgConfigK), tgtHllType, LIST);
}

HllSketch::~HllSketch() {
  delete hllSketchImpl;
}

HllSketch::HllSketch(const HllSketch& that) {
  hllSketchImpl = that.hllSketchImpl->copy();
}

HllSketch::HllSketch(HllSketchImpl* that) {
  hllSketchImpl = that;
}

HllSketch* HllSketch::copy() {
  return new HllSketch(*this);
}

HllSketch* HllSketch::copyAs(const TgtHllType tgtHllType) {
  return new HllSketch(hllSketchImpl->copyAs(tgtHllType));
}

void HllSketch::reset() {
  hllSketchImpl = hllSketchImpl->reset();
}

void HllSketch::couponUpdate(int coupon) {
  if (coupon == HllUtil::EMPTY) { return; }
  HllSketchImpl* result = this->hllSketchImpl->couponUpdate(coupon);
  if (result != this->hllSketchImpl) {
    delete this->hllSketchImpl;
    this->hllSketchImpl = result;
  }
}

void dump_sketch(HllSketch& sketch, const bool all) {
  //std::ostringstream oss;
  //sketch.to_string(oss, true, true, true, all);
  sketch.to_string(std::cout, true, true, true, all);
  //fprintf(stdout, "%s\n", oss.str().c_str());
}

std::ostream& operator<<(std::ostream& os, HllSketch& sketch) {
  return sketch.to_string(os, true, true, false, false);
}

std::ostream& HllSketch::to_string(std::ostream& os, const bool summary,
                                   const bool detail, const bool auxDetail, const bool all) {
  if (summary) {
    os << "### HLL SKETCH SUMMARY: " << std::endl
       << "  Log Config K   : " << getLgConfigK() << std::endl
       << "  Hll Target     : " << type_as_string() << std::endl
       << "  Current Mode   : " << mode_as_string() << std::endl
       << "  LB             : " << getLowerBound(1) << std::endl
       << "  Estimate       : " << getEstimate() << std::endl
       << "  UB             : " << getUpperBound(1) << std::endl
       << "  OutOfOrder flag: " << isOutOfOrderFlag() << std::endl;
    if (getCurMode() == HLL) {
      HllArray* hllArray = (HllArray*) hllSketchImpl;
      os << "  CurMin       : " << hllArray->getCurMin() << std::endl
         << "  NumAtCurMin  : " << hllArray->getNumAtCurMin() << std::endl
         << "  HipAccum     : " << hllArray->getHipAccum() << std::endl
         << "  KxQ0         : " << hllArray->getKxQ0() << std::endl
         << "  KxQ1         : " << hllArray->getKxQ1() << std::endl;
    } else {
      os << "  Coupon count : "
         << std::to_string(((AbstractCoupons*) hllSketchImpl)->getCouponCount()) << std::endl;
    }
  }

  if (detail) {
    os << "### HLL SKETCH DATA DETAIL: " << std::endl;
    std::unique_ptr<PairIterator> pitr = getIterator();
    os << pitr->getHeader() << std::endl;
    if (all) {
      while (pitr->nextAll()) {
        os << pitr->getString() << std::endl;
      }
    } else {
      while (pitr->nextValid()) {
        os << pitr->getString() << std::endl;
      }
    }
  }
  if (auxDetail) {
    if ((getCurMode() == HLL) && (getTgtHllType() == HLL_4)) {
      HllArray* hllArray = (HllArray*) hllSketchImpl;
      std::unique_ptr<PairIterator> auxItr = hllArray->getAuxIterator();
      if (auxItr != nullptr) {
        os << "### HLL SKETCH AUX DETAIL: " << std::endl
           << auxItr->getHeader() << std::endl;
        if (all) {
          while (auxItr->nextAll()) {
            os << auxItr->getString() << std::endl;
          }
        } else {
          while (auxItr->nextAll()) {
            os << auxItr->getString() << std::endl;
          }
        }
      }
    }
  }

  return os;
}

double HllSketch::getEstimate() {
  return hllSketchImpl->getEstimate();
}

double HllSketch::getCompositeEstimate() {
  return hllSketchImpl->getCompositeEstimate();
}

double HllSketch::getLowerBound(int numStdDev) {
  return hllSketchImpl->getLowerBound(numStdDev);
}

double HllSketch::getUpperBound(int numStdDev) {
  return hllSketchImpl->getUpperBound(numStdDev);
}

CurMode HllSketch::getCurMode() {
  return hllSketchImpl->getCurMode();
}

int HllSketch::getLgConfigK() {
  return hllSketchImpl->getLgConfigK();
}

TgtHllType HllSketch::getTgtHllType() {
  return hllSketchImpl->getTgtHllType();
}

bool HllSketch::isOutOfOrderFlag() {
  return hllSketchImpl->isOutOfOrderFlag();
}

int HllSketch::getUpdatableSerializationBytes() {
  return hllSketchImpl->getUpdatableSerializationBytes();
}

int HllSketch::getCompactSerializationBytes() {
  return hllSketchImpl->getCompactSerializationBytes();
}

bool HllSketch::isCompact() {
  return hllSketchImpl->isCompact();
}

bool HllSketch::isEmpty() {
  return hllSketchImpl->isEmpty();
}

std::unique_ptr<PairIterator> HllSketch::getIterator() {
  return hllSketchImpl->getIterator();
}

std::string HllSketch::type_as_string() {
  switch (hllSketchImpl->getTgtHllType()) {
    case TgtHllType::HLL_4:
      return std::string("HLL_4");
    case TgtHllType::HLL_8:
      return std::string("HLL_8");
    default:
      throw std::runtime_error("Sketch state error: Invalid TgtHllType");
  }
}

std::string HllSketch::mode_as_string() {
  switch (hllSketchImpl->getCurMode()) {
    case LIST:
      return std::string("LIST");
    case SET:
      return std::string("SET");
    case HLL:
      return std::string("HLL");
    default:
      throw std::runtime_error("Sketch state error: Invalid CurMode");
  }
}

int HllSketch::getMaxUpdatableSerializationBytes(const int lgConfigK,
    const TgtHllType tgtHllType) {
  int arrBytes;
  if (tgtHllType == TgtHllType::HLL_4) {
    const int auxBytes = 4 << HllUtil::LG_AUX_ARR_INTS[lgConfigK];
    arrBytes =  HllArray::hll4ArrBytes(lgConfigK) + auxBytes;
  } else { //HLL_8
    arrBytes = HllArray::hll8ArrBytes(lgConfigK);
  }
  return HllUtil::HLL_BYTE_ARR_START + arrBytes;
}


}
