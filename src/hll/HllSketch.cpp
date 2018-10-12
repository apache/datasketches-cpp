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

typedef union {
  int64_t longBytes;
  double doubleBytes;
} longDoubleUnion;

HllSketch* HllSketch::newInstance(const int lgConfigK, const TgtHllType tgtHllType) {
  return new HllSketchPvt(lgConfigK, tgtHllType);
}

HllSketch* HllSketch::deserialize(std::istream& is) {
  return HllSketchPvt::deserialize(is);
}

HllSketch::~HllSketch() {}

HllSketchPvt::HllSketchPvt(const int lgConfigK, const TgtHllType tgtHllType) {
  hllSketchImpl = new CouponList(HllUtil::checkLgK(lgConfigK), tgtHllType, CurMode::LIST); 
}

HllSketchPvt* HllSketchPvt::deserialize(std::istream& is) {
  HllSketchImpl* impl = HllSketchImpl::deserialize(is);
  return new HllSketchPvt(impl);
}

HllSketchPvt::~HllSketchPvt() {
  delete hllSketchImpl;
}

std::ostream& operator<<(std::ostream& os, HllSketch& sketch) {
  return sketch.to_string(os, true, true, false, false);
}

HllSketchPvt::HllSketchPvt(const HllSketch& that) {
  hllSketchImpl = static_cast<HllSketchPvt>(that).hllSketchImpl->copy();
}

HllSketchPvt::HllSketchPvt(HllSketchImpl* that) {
  hllSketchImpl = that;
}

HllSketch* HllSketchPvt::copy() const {
  return new HllSketchPvt(this->hllSketchImpl->copy());
}

HllSketch* HllSketchPvt::copyAs(const TgtHllType tgtHllType) const {
  return new HllSketchPvt(hllSketchImpl->copyAs(tgtHllType));
}

void HllSketchPvt::reset() {
  HllSketchImpl* newImpl = hllSketchImpl->reset();
  delete hllSketchImpl;
  hllSketchImpl = newImpl;
}

void HllSketchPvt::update(const std::string datum) {
  if (datum.empty()) { return; }
  uint64_t hashResult[2];
  HllUtil::hash(datum.c_str(), datum.length(), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const uint64_t datum) {
  uint64_t hashResult[2];
  HllUtil::hash(&datum, sizeof(uint64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const uint32_t datum) {
  uint64_t val = static_cast<uint64_t>(datum);
  uint64_t hashResult[2];
  HllUtil::hash(&val, sizeof(uint64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const uint16_t datum) {
  uint64_t val = static_cast<uint64_t>(datum);
  uint64_t hashResult[2];
  HllUtil::hash(&val, sizeof(uint64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const uint8_t datum) {
  uint64_t val = static_cast<uint64_t>(datum);
  uint64_t hashResult[2];
  HllUtil::hash(&val, sizeof(uint64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const int64_t datum) {
  uint64_t hashResult[2];
  HllUtil::hash(&datum, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const int32_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  uint64_t hashResult[2];
  HllUtil::hash(&val, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const int16_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  uint64_t hashResult[2];
  HllUtil::hash(&val, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const int8_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  uint64_t hashResult[2];
  HllUtil::hash(&val, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const double datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  uint64_t hashResult[2];
  HllUtil::hash(&d, sizeof(double), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const float datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  uint64_t hashResult[2];
  HllUtil::hash(&d, sizeof(double), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::update(const void* data, const size_t lengthBytes) {
  if (data == nullptr) { return; }
  uint64_t hashResult[2];
  HllUtil::hash(data, lengthBytes, HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

void HllSketchPvt::couponUpdate(int coupon) {
  if (coupon == HllUtil::EMPTY) { return; }
  HllSketchImpl* result = this->hllSketchImpl->couponUpdate(coupon);
  if (result != this->hllSketchImpl) {
    delete this->hllSketchImpl;
    this->hllSketchImpl = result;
  }
}

void HllSketchPvt::serializeCompact(std::ostream& os) const {
  return hllSketchImpl->serialize(os, true);
}

void HllSketchPvt::serializeUpdatable(std::ostream& os) const {
  return hllSketchImpl->serialize(os, false);
}

std::ostream& HllSketchPvt::to_string(std::ostream& os,
                                      const bool summary,
                                      const bool detail,
                                      const bool auxDetail,
                                      const bool all) const {
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

double HllSketchPvt::getEstimate() const {
  return hllSketchImpl->getEstimate();
}

double HllSketchPvt::getCompositeEstimate() const {
  return hllSketchImpl->getCompositeEstimate();
}

double HllSketchPvt::getLowerBound(int numStdDev) const {
  return hllSketchImpl->getLowerBound(numStdDev);
}

double HllSketchPvt::getUpperBound(int numStdDev) const {
  return hllSketchImpl->getUpperBound(numStdDev);
}

CurMode HllSketchPvt::getCurrentMode() const {
  return hllSketchImpl->getCurMode();
}

int HllSketchPvt::getLgConfigK() const {
  return hllSketchImpl->getLgConfigK();
}

TgtHllType HllSketchPvt::getTgtHllType() const {
  return hllSketchImpl->getTgtHllType();
}

bool HllSketchPvt::isOutOfOrderFlag() const {
  return hllSketchImpl->isOutOfOrderFlag();
}

bool HllSketchPvt::isEstimationMode() const {
  return true;
}

int HllSketchPvt::getUpdatableSerializationBytes() const {
  return hllSketchImpl->getUpdatableSerializationBytes();
}

int HllSketchPvt::getCompactSerializationBytes() const {
  return hllSketchImpl->getCompactSerializationBytes();
}

bool HllSketchPvt::isCompact() const {
  return hllSketchImpl->isCompact();
}

bool HllSketchPvt::isEmpty() const {
  return hllSketchImpl->isEmpty();
}

std::unique_ptr<PairIterator> HllSketchPvt::getIterator() const {
  return hllSketchImpl->getIterator();
}

std::string HllSketchPvt::typeAsString() const {
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

std::string HllSketchPvt::modeAsString() const {
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
    arrBytes = HllArray::hll4ArrBytes(lgConfigK) + auxBytes;
  } else if (tgtHllType == TgtHllType::HLL_6) {
    arrBytes = HllArray::hll6ArrBytes(lgConfigK);
  } else { //HLL_8
    arrBytes = HllArray::hll8ArrBytes(lgConfigK);
  }
  return HllUtil::HLL_BYTE_ARR_START + arrBytes;
}

double HllSketch::getRelErr(const bool upperBound, const bool unioned,
                           const int lgConfigK, const int numStdDev) {
  return HllUtil::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

}
