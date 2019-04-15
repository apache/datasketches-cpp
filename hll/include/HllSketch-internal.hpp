/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLSKETCH_INTERNAL_HPP_
#define _HLLSKETCH_INTERNAL_HPP_

#include "HllSketch.hpp"
#include "HllUtil.hpp"
#include "CouponList.hpp"
#include "HllArray.hpp"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <sstream>

namespace datasketches {

typedef union {
  int64_t longBytes;
  double doubleBytes;
} longDoubleUnion;

template<typename A>
HllSketch<A> HllSketch<A>::newInstance(const int lgConfigK, const TgtHllType tgtHllType) {
  /*
  std::unique_ptr<HllSketch<A>, std::function<void(HllSketch<A>*)>> sketch_ptr(
      AllocHllSketch.allocate(1)
      [](HllSketch<A>* s) { s->~HllSketch(); AllocHllSketch().deallocate(s, 1); }
  AllocHllSketch.construct(sketch_ptr->get(), lgConfigK, tgtHllType);
  return sketch_ptr;
  */
  HllSketch<A> sketch(lgConfigK, tgtHllType);
  return sketch;
}

template<typename A>
HllSketch<A> HllSketch<A>::deserialize(std::istream& is) {
  return HllSketchPvt<A>::deserialize(is);
}

template<typename A>
HllSketch<A> HllSketch<A>::deserialize(const void* bytes, size_t len) {
  return HllSketchPvt<A>::deserialize(bytes, len);
}

template<typename A>
HllSketch<A> HllSketch<A>::copy() const {
  return static_cast<const HllSketchPvt<A>>(this)->copy();
}

template<typename A>
HllSketch<A> HllSketch<A>::copyAs(TgtHllType tgtHllType) const {
  return static_cast<const HllSketchPvt<A>>(this)->copyAs(tgtHllType);
}

template<typename A>
HllSketch<A>::~HllSketch() {}

template<typename A>
HllSketchPvt<A>::HllSketchPvt(const int lgConfigK, const TgtHllType tgtHllType) {
  hllSketchImpl = new CouponList(HllUtil::checkLgK(lgConfigK), tgtHllType, CurMode::LIST); 
}

template<typename A>
HllSketchPvt<A> HllSketchPvt<A>::deserialize(std::istream& is) {
  HllSketchImpl* impl = HllSketchImpl::deserialize(is);
  HllSketchPvt<A> sketch = AllocHllSketch::allocate(1);
  AllocHllSketch::construct(sketch, impl);
  return sketch;
}

template<typename A>
HllSketchPvt<A> HllSketchPvt<A>::deserialize(const void* bytes, size_t len) {
  HllSketchImpl* impl = HllSketchImpl::deserialize(bytes, len);
  HllSketchPvt<A> sketch = AllocHllSketch::allocate(1);
  AllocHllSketch::construct(sketch, impl);
  return sketch;
}

template<typename A>
HllSketchPvt<A>::~HllSketchPvt() {
  this->hllSketchImpl->~hllSketchImpl();
  A::deallocate(hllSketchImpl, 1);
}

template<typename A>
std::ostream& operator<<(std::ostream& os, HllSketch<A>& sketch) {
  return sketch.to_string(os, true, true, false, false);
}

/*
template<typename A>
std::ostream& operator<<(std::ostream& os, hll_sketch& sketch) {
  return sketch->to_string(os, true, true, false, false);
}
*/

template<typename A>
HllSketchPvt<A>::HllSketchPvt(const HllSketchPvt& that) :
  hllSketchImpl(static_cast<HllSketchPvt<A>>(that).hllSketchImpl->copy())
{}

template<typename A>
HllSketchPvt<A>::HllSketchPvt(HllSketchImpl* that) :
  hllSketchImpl(that)
{}

template<typename A>
HllSketchPvt<A>& HllSketchPvt<A>::operator=(HllSketchPvt other) {
  std::swap(hllSketchImpl, other.hllSketchImpl);
  return *this;
}

template<typename A>
HllSketchPvt<A> HllSketchPvt<A>::copy() const {
  HllSketchPvt<A> sketch = AllocHllSketch::allocate(1);
  AllocHllSketch::construct(sketch, this->hllSketchImpl->copy());
  return sketch;
  //return std::unique_ptr<HllSketchPvt>(new HllSketchPvt(this->hllSketchImpl->copy()));
}

template<typename A>
HllSketchPvt<A> HllSketchPvt<A>::copyAs(const TgtHllType tgtHllType) const {
  HllSketchPvt<A> sketch = AllocHllSketch::allocate(1);
  AllocHllSketch::construct(sketch, this->hllSketchImpl->copy());
  return sketch;
  //return std::unique_ptr<HllSketchPvt>(new HllSketchPvt(hllSketchImpl->copyAs(tgtHllType)));
}

template<typename A>
void HllSketchPvt<A>::reset() {
  HllSketchImpl* newImpl = hllSketchImpl->reset();
  delete hllSketchImpl;
  hllSketchImpl = newImpl;
}

template<typename A>
void HllSketchPvt<A>::update(const std::string& datum) {
  if (datum.empty()) { return; }
  HashState hashResult;
  HllUtil::hash(datum.c_str(), datum.length(), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const uint64_t datum) {
  // no sign extension with 64 bits so no need to cast to signed value
  HashState hashResult;
  HllUtil::hash(&datum, sizeof(uint64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const uint32_t datum) {
  update(static_cast<int32_t>(datum));
}

template<typename A>
void HllSketchPvt<A>::update(const uint16_t datum) {
  update(static_cast<int16_t>(datum));
}

template<typename A>
void HllSketchPvt<A>::update(const uint8_t datum) {
  update(static_cast<int8_t>(datum));
}

template<typename A>
void HllSketchPvt<A>::update(const int64_t datum) {
  HashState hashResult;
  HllUtil::hash(&datum, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const int32_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil::hash(&val, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const int16_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil::hash(&val, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const int8_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil::hash(&val, sizeof(int64_t), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const double datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  HashState hashResult;
  HllUtil::hash(&d, sizeof(double), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const float datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  HashState hashResult;
  HllUtil::hash(&d, sizeof(double), HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::update(const void* data, const size_t lengthBytes) {
  if (data == nullptr) { return; }
  HashState hashResult;
  HllUtil::hash(data, lengthBytes, HllUtil::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil::coupon(hashResult));
}

template<typename A>
void HllSketchPvt<A>::couponUpdate(int coupon) {
  if (coupon == HllUtil::EMPTY) { return; }
  HllSketchImpl* result = this->hllSketchImpl->couponUpdate(coupon);
  if (result != this->hllSketchImpl) {
    delete this->hllSketchImpl;
    this->hllSketchImpl = result;
  }
}

template<typename A>
void HllSketchPvt<A>::serializeCompact(std::ostream& os) const {
  return hllSketchImpl->serialize(os, true);
}

template<typename A>
void HllSketchPvt<A>::serializeUpdatable(std::ostream& os) const {
  return hllSketchImpl->serialize(os, false);
}

template<typename A>
std::pair<std::unique_ptr<uint8_t[]>, const size_t>
HllSketchPvt<A>::serializeCompact() const {
  return hllSketchImpl->serialize(true);
}

template<typename A>
std::pair<std::unique_ptr<uint8_t[]>, const size_t>
HllSketchPvt<A>::serializeUpdatable() const {
  return hllSketchImpl->serialize(false);
}

template<typename A>
std::string HllSketchPvt<A>::to_string(const bool summary,
                                    const bool detail,
                                    const bool auxDetail,
                                    const bool all) const {
  std::ostringstream oss;
  to_string(oss, summary, detail, auxDetail, all);
  return oss.str();
}

template<typename A>
std::ostream& HllSketchPvt<A>::to_string(std::ostream& os,
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

template<typename A>
double HllSketchPvt<A>::getEstimate() const {
  return hllSketchImpl->getEstimate();
}

template<typename A>
double HllSketchPvt<A>::getCompositeEstimate() const {
  return hllSketchImpl->getCompositeEstimate();
}

template<typename A>
double HllSketchPvt<A>::getLowerBound(int numStdDev) const {
  return hllSketchImpl->getLowerBound(numStdDev);
}

template<typename A>
double HllSketchPvt<A>::getUpperBound(int numStdDev) const {
  return hllSketchImpl->getUpperBound(numStdDev);
}

template<typename A>
CurMode HllSketchPvt<A>::getCurrentMode() const {
  return hllSketchImpl->getCurMode();
}

template<typename A>
int HllSketchPvt<A>::getLgConfigK() const {
  return hllSketchImpl->getLgConfigK();
}

template<typename A>
TgtHllType HllSketchPvt<A>::getTgtHllType() const {
  return hllSketchImpl->getTgtHllType();
}

template<typename A>
bool HllSketchPvt<A>::isOutOfOrderFlag() const {
  return hllSketchImpl->isOutOfOrderFlag();
}

template<typename A>
bool HllSketchPvt<A>::isEstimationMode() const {
  return true;
}

template<typename A>
int HllSketchPvt<A>::getUpdatableSerializationBytes() const {
  return hllSketchImpl->getUpdatableSerializationBytes();
}

template<typename A>
int HllSketchPvt<A>::getCompactSerializationBytes() const {
  return hllSketchImpl->getCompactSerializationBytes();
}

template<typename A>
bool HllSketchPvt<A>::isCompact() const {
  return hllSketchImpl->isCompact();
}

template<typename A>
bool HllSketchPvt<A>::isEmpty() const {
  return hllSketchImpl->isEmpty();
}

template<typename A>
std::unique_ptr<PairIterator> HllSketchPvt<A>::getIterator() const {
  return hllSketchImpl->getIterator();
}

template<typename A>
std::string HllSketchPvt<A>::typeAsString() const {
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

template<typename A>
std::string HllSketchPvt<A>::modeAsString() const {
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

template<typename A>
int HllSketch<A>::getMaxUpdatableSerializationBytes(const int lgConfigK,
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

template<typename A>
double HllSketch<A>::getRelErr(const bool upperBound, const bool unioned,
                           const int lgConfigK, const int numStdDev) {
  return HllUtil::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

}

#endif // _HLLSKETCH_INTERNAL_HPP_
