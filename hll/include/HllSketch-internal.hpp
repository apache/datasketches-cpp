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

#ifndef _HLLSKETCH_INTERNAL_HPP_
#define _HLLSKETCH_INTERNAL_HPP_

#include "hll.hpp"
#include "HllUtil.hpp"
#include "HllSketchImplFactory.hpp"
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
hll_sketch_alloc<A>::hll_sketch_alloc(int lgConfigK, target_hll_type tgtHllType, bool startFullSize) {
  HllUtil<A>::checkLgK(lgConfigK);
  if (startFullSize) {
    hllSketchImpl = HllSketchImplFactory<A>::newHll(lgConfigK, tgtHllType, startFullSize);
  } else {
    typedef typename std::allocator_traits<A>::template rebind_alloc<CouponList<A>> clAlloc;
    hllSketchImpl = new (clAlloc().allocate(1)) CouponList<A>(lgConfigK, tgtHllType, CurMode::LIST);
  }
}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::deserialize(std::istream& is) {
  HllSketchImpl<A>* impl = HllSketchImplFactory<A>::deserialize(is);
  hll_sketch_alloc<A> sketch(impl);
  return sketch;
}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::deserialize(const void* bytes, size_t len) {
  HllSketchImpl<A>* impl = HllSketchImplFactory<A>::deserialize(bytes, len);
  hll_sketch_alloc<A> sketch(impl);
  return sketch;
}

template<typename A>
hll_sketch_alloc<A>::~hll_sketch_alloc() {
  if (hllSketchImpl != nullptr) {
    hllSketchImpl->get_deleter()(hllSketchImpl);
  }
}

template<typename A>
std::ostream& operator<<(std::ostream& os, const hll_sketch_alloc<A>& sketch) {
  return sketch.to_string(os, true, true, false, false);
}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(const hll_sketch_alloc<A>& that) :
  hllSketchImpl(that.hllSketchImpl->copy())
{}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(const hll_sketch_alloc<A>& that, target_hll_type tgtHllType) :
  hllSketchImpl(that.hllSketchImpl->copyAs(tgtHllType))
{}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(hll_sketch_alloc<A>&& that) noexcept :
  hllSketchImpl(nullptr)
{
  std::swap(hllSketchImpl, that.hllSketchImpl);
}

template<typename A>
hll_sketch_alloc<A>::hll_sketch_alloc(HllSketchImpl<A>* that) :
  hllSketchImpl(that)
{}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::operator=(const hll_sketch_alloc<A>& other) {
  hllSketchImpl->get_deleter()(hllSketchImpl);
  hllSketchImpl = other.hllSketchImpl->copy();
  return *this;
}

template<typename A>
hll_sketch_alloc<A> hll_sketch_alloc<A>::operator=(hll_sketch_alloc<A>&& other) {
  std::swap(hllSketchImpl, other.hllSketchImpl);
  return *this;
}

template<typename A>
void hll_sketch_alloc<A>::reset() {
  // TODO: need to allow starting from a full-sized sketch
  //       (either here or in other implementation)
  hllSketchImpl = hllSketchImpl->reset();
}

template<typename A>
void hll_sketch_alloc<A>::update(const std::string& datum) {
  if (datum.empty()) { return; }
  HashState hashResult;
  HllUtil<A>::hash(datum.c_str(), datum.length(), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(const uint64_t datum) {
  // no sign extension with 64 bits so no need to cast to signed value
  HashState hashResult;
  HllUtil<A>::hash(&datum, sizeof(uint64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(const uint32_t datum) {
  update(static_cast<int32_t>(datum));
}

template<typename A>
void hll_sketch_alloc<A>::update(const uint16_t datum) {
  update(static_cast<int16_t>(datum));
}

template<typename A>
void hll_sketch_alloc<A>::update(const uint8_t datum) {
  update(static_cast<int8_t>(datum));
}

template<typename A>
void hll_sketch_alloc<A>::update(const int64_t datum) {
  HashState hashResult;
  HllUtil<A>::hash(&datum, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(const int32_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(const int16_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(const int8_t datum) {
  int64_t val = static_cast<int64_t>(datum);
  HashState hashResult;
  HllUtil<A>::hash(&val, sizeof(int64_t), HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::update(const double datum) {
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
void hll_sketch_alloc<A>::update(const float datum) {
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
void hll_sketch_alloc<A>::update(const void* data, const size_t lengthBytes) {
  if (data == nullptr) { return; }
  HashState hashResult;
  HllUtil<A>::hash(data, lengthBytes, HllUtil<A>::DEFAULT_UPDATE_SEED, hashResult);
  couponUpdate(HllUtil<A>::coupon(hashResult));
}

template<typename A>
void hll_sketch_alloc<A>::couponUpdate(int coupon) {
  if (coupon == HllUtil<A>::EMPTY) { return; }
  HllSketchImpl<A>* result = this->hllSketchImpl->couponUpdate(coupon);
  if (result != this->hllSketchImpl) {
    this->hllSketchImpl->get_deleter()(this->hllSketchImpl);
    this->hllSketchImpl = result;
  }
}

template<typename A>
void hll_sketch_alloc<A>::serialize_compact(std::ostream& os) const {
  return hllSketchImpl->serialize(os, true);
}

template<typename A>
void hll_sketch_alloc<A>::serialize_updatable(std::ostream& os) const {
  return hllSketchImpl->serialize(os, false);
}

template<typename A>
std::pair<std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>, const size_t>
hll_sketch_alloc<A>::serialize_compact(unsigned header_size_bytes) const {
  return hllSketchImpl->serialize(true, header_size_bytes);
}

template<typename A>
std::pair<std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>, const size_t>
hll_sketch_alloc<A>::serialize_updatable() const {
  return hllSketchImpl->serialize(false, 0);
}

template<typename A>
std::string hll_sketch_alloc<A>::to_string(const bool summary,
                                    const bool detail,
                                    const bool auxDetail,
                                    const bool all) const {
  std::ostringstream oss;
  to_string(oss, summary, detail, auxDetail, all);
  return oss.str();
}

template<typename A>
std::ostream& hll_sketch_alloc<A>::to_string(std::ostream& os,
                                      const bool summary,
                                      const bool detail,
                                      const bool auxDetail,
                                      const bool all) const {
  if (summary) {
    os << "### HLL SKETCH SUMMARY: " << std::endl
       << "  Log Config K   : " << get_lg_config_k() << std::endl
       << "  Hll Target     : " << typeAsString() << std::endl
       << "  Current Mode   : " << modeAsString() << std::endl
       << "  LB             : " << get_lower_bound(1) << std::endl
       << "  Estimate       : " << get_estimate() << std::endl
       << "  UB             : " << get_upper_bound(1) << std::endl
       << "  OutOfOrder flag: " << isOutOfOrderFlag() << std::endl;
    if (getCurrentMode() == HLL) {
      HllArray<A>* hllArray = (HllArray<A>*) hllSketchImpl;
      os << "  CurMin       : " << hllArray->getCurMin() << std::endl
         << "  NumAtCurMin  : " << hllArray->getNumAtCurMin() << std::endl
         << "  HipAccum     : " << hllArray->getHipAccum() << std::endl
         << "  KxQ0         : " << hllArray->getKxQ0() << std::endl
         << "  KxQ1         : " << hllArray->getKxQ1() << std::endl;
    } else {
      os << "  Coupon count : "
         << std::to_string(((CouponList<A>*) hllSketchImpl)->getCouponCount()) << std::endl;
    }
  }

  if (detail) {
    os << "### HLL SKETCH DATA DETAIL: " << std::endl;
    pair_iterator_with_deleter<A> pitr = get_iterator();
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
    if ((getCurrentMode() == HLL) && (get_target_type() == HLL_4)) {
      HllArray<A>* hllArray = (HllArray<A>*) hllSketchImpl;
      pair_iterator_with_deleter<A> auxItr = hllArray->getAuxIterator();
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
double hll_sketch_alloc<A>::get_estimate() const {
  return hllSketchImpl->getEstimate();
}

template<typename A>
double hll_sketch_alloc<A>::get_composite_estimate() const {
  return hllSketchImpl->getCompositeEstimate();
}

template<typename A>
double hll_sketch_alloc<A>::get_lower_bound(int numStdDev) const {
  return hllSketchImpl->getLowerBound(numStdDev);
}

template<typename A>
double hll_sketch_alloc<A>::get_upper_bound(int numStdDev) const {
  return hllSketchImpl->getUpperBound(numStdDev);
}

template<typename A>
CurMode hll_sketch_alloc<A>::getCurrentMode() const {
  return hllSketchImpl->getCurMode();
}

template<typename A>
int hll_sketch_alloc<A>::get_lg_config_k() const {
  return hllSketchImpl->getLgConfigK();
}

template<typename A>
target_hll_type hll_sketch_alloc<A>::get_target_type() const {
  return hllSketchImpl->getTgtHllType();
}

template<typename A>
bool hll_sketch_alloc<A>::isOutOfOrderFlag() const {
  return hllSketchImpl->isOutOfOrderFlag();
}

template<typename A>
bool hll_sketch_alloc<A>::isEstimationMode() const {
  return true;
}

template<typename A>
int hll_sketch_alloc<A>::get_updatable_serialization_bytes() const {
  return hllSketchImpl->getUpdatableSerializationBytes();
}

template<typename A>
int hll_sketch_alloc<A>::get_compact_serialization_bytes() const {
  return hllSketchImpl->getCompactSerializationBytes();
}

template<typename A>
bool hll_sketch_alloc<A>::is_compact() const {
  return hllSketchImpl->isCompact();
}

template<typename A>
bool hll_sketch_alloc<A>::is_empty() const {
  return hllSketchImpl->isEmpty();
}

template<typename A>
pair_iterator_with_deleter<A> hll_sketch_alloc<A>::get_iterator() const {
  return hllSketchImpl->getIterator();
}

template<typename A>
std::string hll_sketch_alloc<A>::typeAsString() const {
  switch (hllSketchImpl->getTgtHllType()) {
    case target_hll_type::HLL_4:
      return std::string("HLL_4");
    case target_hll_type::HLL_6:
      return std::string("HLL_6");
    case target_hll_type::HLL_8:
      return std::string("HLL_8");
    default:
      throw std::runtime_error("Sketch state error: Invalid target_hll_type");
  }
}

template<typename A>
std::string hll_sketch_alloc<A>::modeAsString() const {
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
int hll_sketch_alloc<A>::get_max_updatable_serialization_bytes(const int lgConfigK,
    const target_hll_type tgtHllType) {
  int arrBytes;
  if (tgtHllType == target_hll_type::HLL_4) {
    const int auxBytes = 4 << HllUtil<A>::LG_AUX_ARR_INTS[lgConfigK];
    arrBytes = HllArray<A>::hll4ArrBytes(lgConfigK) + auxBytes;
  } else if (tgtHllType == target_hll_type::HLL_6) {
    arrBytes = HllArray<A>::hll6ArrBytes(lgConfigK);
  } else { //HLL_8
    arrBytes = HllArray<A>::hll8ArrBytes(lgConfigK);
  }
  return HllUtil<A>::HLL_BYTE_ARR_START + arrBytes;
}

template<typename A>
double hll_sketch_alloc<A>::get_rel_err(const bool upperBound, const bool unioned,
                           const int lgConfigK, const int numStdDev) {
  return HllUtil<A>::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

}

#endif // _HLLSKETCH_INTERNAL_HPP_
