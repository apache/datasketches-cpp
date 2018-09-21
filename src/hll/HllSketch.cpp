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

HllSketch* HllSketch::deserialize(std::istream& is) {
  HllSketchImpl* impl = HllSketchImpl::deserialize(is);
  return new HllSketch(impl);
}

HllSketch* HllSketch::copy() const {
  return new HllSketch(*this);
}

HllSketch* HllSketch::copyAs(const TgtHllType tgtHllType) const {
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

std::ostream& operator<<(std::ostream& os, HllSketch& sketch) {
  return sketch.to_string(os, true, true, false, false);
}

void HllSketch::serializeCompact(std::ostream& os) const {
  return hllSketchImpl->serialize(os, true);
}

void HllSketch::serializeUpdatable(std::ostream& os) const {
  return hllSketchImpl->serialize(os, false);
}

std::ostream& HllSketch::to_string(std::ostream& os, const bool summary,
                                   const bool detail, const bool auxDetail, const bool all) const {
  if (summary) {
    os << "### HLL SKETCH SUMMARY: " << std::endl
       << "  Log Config K   : " << getLgConfigK() << std::endl
       << "  Hll Target     : " << typeAsString() << std::endl
       << "  Current Mode   : " << modeAsString() << std::endl
       << "  LB             : " << getLowerBound(1) << std::endl
       << "  Estimate       : " << getEstimate() << std::endl
       << "  UB             : " << getUpperBound(1) << std::endl
       << "  OutOfOrder flag: " << isOutOfOrderFlag() << std::endl;
    if (getCurrentMode() == HLL) {
      HllArray* hllArray = (HllArray*) hllSketchImpl;
      os << "  CurMin       : " << hllArray->getCurMin() << std::endl
         << "  NumAtCurMin  : " << hllArray->getNumAtCurMin() << std::endl
         << "  HipAccum     : " << hllArray->getHipAccum() << std::endl
         << "  KxQ0         : " << hllArray->getKxQ0() << std::endl
         << "  KxQ1         : " << hllArray->getKxQ1() << std::endl;
    } else {
      os << "  Coupon count : "
         << std::to_string(((CouponList*) hllSketchImpl)->getCouponCount()) << std::endl;
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
    if ((getCurrentMode() == HLL) && (getTgtHllType() == HLL_4)) {
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

double HllSketch::getEstimate() const {
  return hllSketchImpl->getEstimate();
}

double HllSketch::getCompositeEstimate() const {
  return hllSketchImpl->getCompositeEstimate();
}

double HllSketch::getLowerBound(int numStdDev) const {
  return hllSketchImpl->getLowerBound(numStdDev);
}

double HllSketch::getUpperBound(int numStdDev) const {
  return hllSketchImpl->getUpperBound(numStdDev);
}

CurMode HllSketch::getCurrentMode() const {
  return hllSketchImpl->getCurMode();
}

int HllSketch::getLgConfigK() const {
  return hllSketchImpl->getLgConfigK();
}

TgtHllType HllSketch::getTgtHllType() const {
  return hllSketchImpl->getTgtHllType();
}

bool HllSketch::isOutOfOrderFlag() const {
  return hllSketchImpl->isOutOfOrderFlag();
}

int HllSketch::getUpdatableSerializationBytes() const {
  return hllSketchImpl->getUpdatableSerializationBytes();
}

int HllSketch::getCompactSerializationBytes() const {
  return hllSketchImpl->getCompactSerializationBytes();
}

bool HllSketch::isCompact() const {
  return hllSketchImpl->isCompact();
}

bool HllSketch::isEmpty() const {
  return hllSketchImpl->isEmpty();
}

std::unique_ptr<PairIterator> HllSketch::getIterator() const {
  return hllSketchImpl->getIterator();
}

std::string HllSketch::typeAsString() const {
  switch (hllSketchImpl->getTgtHllType()) {
    case TgtHllType::HLL_4:
      return std::string("HLL_4");
    case TgtHllType::HLL_6:
      return std::string("HLL_6");
    case TgtHllType::HLL_8:
      return std::string("HLL_8");
    default:
      throw std::runtime_error("Sketch state error: Invalid TgtHllType");
  }
}

std::string HllSketch::modeAsString() const {
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
