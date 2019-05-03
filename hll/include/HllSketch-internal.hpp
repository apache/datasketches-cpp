/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLSKETCH_INTERNAL_HPP_
#define _HLLSKETCH_INTERNAL_HPP_

//#include "HllSketch.hpp"
#include "hll.hpp"
#include "HllUtil.hpp"
#include "HllSketchImplFactory.hpp"
//#include "CouponMode.hpp"
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
HllSketch<A>::HllSketch(int lgConfigK, TgtHllType tgtHllType) {
  // TODO: use allocator
  hllSketchImpl = new CouponList(HllUtil<A>::checkLgK(lgConfigK), tgtHllType, CurMode::LIST); 
}

template<typename A>
HllSketch<A> HllSketch<A>::deserialize(std::istream& is) {
  HllSketchImpl* impl = HllSketchImplFactory::deserialize(is);
  HllSketch<A> sketch(impl);
  return sketch;
}

template<typename A>
HllSketch<A> HllSketch<A>::deserialize(const void* bytes, size_t len) {
  HllSketchImpl* impl = HllSketchImplFactory::deserialize(bytes, len);
  HllSketch<A> sketch(impl);
  return sketch;
}

template<typename A>
HllSketch<A>::~HllSketch() {
  this->hllSketchImpl->~HllSketchImpl();
  typedef typename std::allocator_traits<A>::template rebind_alloc<HllSketchImpl> AllocImpl;
  // TODO: HllSketchImpl is abstract -- need a concrete instance when deallocating?
  AllocImpl().deallocate(hllSketchImpl, 1);
}

template<typename A>
std::ostream& operator<<(std::ostream& os, HllSketch<A>& sketch) {
  return sketch.to_string(os, true, true, false, false);
}

template<typename A>
HllSketch<A>::HllSketch(const HllSketch<A>& that) :
  hllSketchImpl(that.hllSketchImpl->copy())
{}

template<typename A>
HllSketch<A>::HllSketch(HllSketchImpl* that) :
  hllSketchImpl(that)
{}

template<typename A>
HllSketch<A> HllSketch<A>::operator=(HllSketch<A>& other) {
  this->hllSketchImpl->~HllSketchImpl();
  typedef typename std::allocator_traits<A>::template rebind_alloc<HllSketchImpl> AllocImpl;
  // TODO: HllSketchImpl is abstract -- need a concrete instance when deallocating?
  AllocImpl().deallocate(hllSketchImpl, 1);
  hllSketchImpl = other.hllSketchImpl->copy();
  return *this;
}

template<typename A>
HllSketch<A> HllSketch<A>::operator=(HllSketch<A>&& other) {
  this->hllSketchImpl->~HllSketchImpl();
  typedef typename std::allocator_traits<A>::template rebind_alloc<HllSketchImpl> AllocImpl;
  // TODO: HllSketchImpl is abstract -- need a concrete instance when deallocating?
  AllocImpl().deallocate(hllSketchImpl, 1);
  hllSketchImpl = std::move(other.hllSketchImpl);
  return *this;
}

template<typename A>
HllSketch<A> HllSketch<A>::copy() const {
  HllSketch<A>* sketch = AllocHllSketch().allocate(1);
  AllocHllSketch().construct(sketch, this->hllSketchImpl->copy());
  return std::move(*sketch);
}

template<typename A>
HllSketch<A>* HllSketch<A>::copyPtr() const {
  HllSketch<A>* sketch = AllocHllSketch().allocate(1);
  AllocHllSketch().construct(sketch, this->hllSketchImpl->copy());
  return sketch;
}

template<typename A>
HllSketch<A> HllSketch<A>::copyAs(const TgtHllType tgtHllType) const {
  HllSketch<A>* sketch = AllocHllSketch().allocate(1);
  AllocHllSketch().construct(sketch, this->hllSketchImpl->copy());
  return std::move(*sketch);
}

template<typename A>
void HllSketch<A>::reset() {
  // TODO: use allocator, probably by dispatching to factory?
  HllSketchImpl* newImpl = hllSketchImpl->reset();
  delete hllSketchImpl;
  hllSketchImpl = newImpl;
}

template<typename A>
void HllSketch<A>::update(const std::string& datum) {
  if (datum.empty()) { return; }
  HashState hashResult;
  HllUtil<A>::hash(datum.c_str(), datum.length(), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const uint64_t datum) {
  // no sign extension with 64 bits so no need to cast to signed value
  HashState hashResult;
  HllUtil<A>::hash(&datum, sizeof(uint64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const uint32_t datum) {
  update(static_cast<int32_t>(datum));
}

template<typename A>
void HllSketch<A>::update(const uint16_t datum) {
  update(static_cast<int16_t>(datum));
}

template<typename A>
void HllSketch<A>::update(const uint8_t datum) {
  update(static_cast<int8_t>(datum));
}

template<typename A>
void HllSketch<A>::update(const int64_t datum) {
  HashState hashResult;
  HllUtil<A>::hash(&datum, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const int32_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const int16_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const int8_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const double datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  HashState hashResult;
  HllUtil<A>::hash(&d, sizeof(double), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const float datum) {
  longDoubleUnion d;
  d.doubleBytes = static_cast<double>(datum);
  if (datum == 0.0) {
    d.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(d.doubleBytes)) {
    d.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  HashState hashResult;
  HllUtil<A>::hash(&d, sizeof(double), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::update(const void* data, const size_t lengthBytes) {
  if (data == nullptr) { return; }
  HashState hashResult;
  HllUtil<A>::hash(data, lengthBytes, HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void HllSketch<A>::couponUpdate(int coupon) {
  if (coupon == HllUtil<A>::EMPTY) { return; }
  HllSketchImpl* result = this->hllSketchImpl->couponUpdate(coupon);
  if (result != this->hllSketchImpl) {
    delete this->hllSketchImpl;
    this->hllSketchImpl = result;
  }
}

template<typename A>
void HllSketch<A>::serializeCompact(std::ostream& os) const {
  return hllSketchImpl->serialize(os, true);
}

template<typename A>
void HllSketch<A>::serializeUpdatable(std::ostream& os) const {
  return hllSketchImpl->serialize(os, false);
}

template<typename A>
std::pair<std::unique_ptr<uint8_t>, const size_t>
HllSketch<A>::serializeCompact() const {
  return hllSketchImpl->serialize(true);
}

template<typename A>
std::pair<std::unique_ptr<uint8_t>, const size_t>
HllSketch<A>::serializeUpdatable() const {
  return hllSketchImpl->serialize(false);
}

template<typename A>
std::string HllSketch<A>::to_string(const bool summary,
                                    const bool detail,
                                    const bool auxDetail,
                                    const bool all) const {
  std::ostringstream oss;
  to_string(oss, summary, detail, auxDetail, all);
  return oss.str();
}

template<typename A>
std::ostream& HllSketch<A>::to_string(std::ostream& os,
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
double HllSketch<A>::getEstimate() const {
  return hllSketchImpl->getEstimate();
}

template<typename A>
double HllSketch<A>::getCompositeEstimate() const {
  return hllSketchImpl->getCompositeEstimate();
}

template<typename A>
double HllSketch<A>::getLowerBound(int numStdDev) const {
  return hllSketchImpl->getLowerBound(numStdDev);
}

template<typename A>
double HllSketch<A>::getUpperBound(int numStdDev) const {
  return hllSketchImpl->getUpperBound(numStdDev);
}

template<typename A>
CurMode HllSketch<A>::getCurrentMode() const {
  return hllSketchImpl->getCurMode();
}

template<typename A>
int HllSketch<A>::getLgConfigK() const {
  return hllSketchImpl->getLgConfigK();
}

template<typename A>
TgtHllType HllSketch<A>::getTgtHllType() const {
  return hllSketchImpl->getTgtHllType();
}

template<typename A>
bool HllSketch<A>::isOutOfOrderFlag() const {
  return hllSketchImpl->isOutOfOrderFlag();
}

template<typename A>
bool HllSketch<A>::isEstimationMode() const {
  return true;
}

template<typename A>
int HllSketch<A>::getUpdatableSerializationBytes() const {
  return hllSketchImpl->getUpdatableSerializationBytes();
}

template<typename A>
int HllSketch<A>::getCompactSerializationBytes() const {
  return hllSketchImpl->getCompactSerializationBytes();
}

template<typename A>
bool HllSketch<A>::isCompact() const {
  return hllSketchImpl->isCompact();
}

template<typename A>
bool HllSketch<A>::isEmpty() const {
  return hllSketchImpl->isEmpty();
}

template<typename A>
std::unique_ptr<PairIterator<A>> HllSketch<A>::getIterator() const {
  return hllSketchImpl->getIterator();
}

template<typename A>
std::string HllSketch<A>::typeAsString() const {
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
std::string HllSketch<A>::modeAsString() const {
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
    const int auxBytes = 4 << HllUtil<A>::LG_AUX_ARR_INTS[lgConfigK];
    arrBytes = HllArray::hll4ArrBytes(lgConfigK) + auxBytes;
  } else if (tgtHllType == TgtHllType::HLL_6) {
    arrBytes = HllArray::hll6ArrBytes(lgConfigK);
  } else { //HLL_8
    arrBytes = HllArray::hll8ArrBytes(lgConfigK);
  }
  return HllUtil<A>::HLL_BYTE_ARR_START + arrBytes;
}

template<typename A>
double HllSketch<A>::getRelErr(const bool upperBound, const bool unioned,
                           const int lgConfigK, const int numStdDev) {
  return HllUtil<A>::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

}

#endif // _HLLSKETCH_INTERNAL_HPP_
