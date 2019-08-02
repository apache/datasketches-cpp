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

#ifndef _HLLUNION_INTERNAL_HPP_
#define _HLLUNION_INTERNAL_HPP_

#include "hll.hpp"

#include "HllSketchImpl.hpp"
#include "HllArray.hpp"
#include "HllUtil.hpp"

#include <stdexcept>
#include <string>

namespace datasketches {

template<typename A>
hll_union_alloc<A>::hll_union_alloc(const int lgMaxK):
  lgMaxK(HllUtil<A>::checkLgK(lgMaxK)),
  gadget(lgMaxK, target_hll_type::HLL_8)
{}

template<typename A>
hll_union_alloc<A> hll_union_alloc<A>::deserialize(const void* bytes, size_t len) {
  hll_sketch_alloc<A> sk(hll_sketch_alloc<A>::deserialize(bytes, len));
  // we're using the sketch's lgConfigK to initialize the union so
  // we can initialize the Union with it as long as it's HLL_8.
  hll_union_alloc<A> hllUnion(sk.get_lg_config_k());
  if (sk.get_target_type() == HLL_8) {
    std::swap(hllUnion.gadget.hllSketchImpl, sk.hllSketchImpl);
  } else {
    hllUnion.update(sk);
  }
  return hllUnion;
}

template<typename A>
hll_union_alloc<A> hll_union_alloc<A>::deserialize(std::istream& is) {
  hll_sketch_alloc<A> sk(hll_sketch_alloc<A>::deserialize(is));
  // we're using the sketch's lgConfigK to initialize the union so
  // we can initialize the Union with it as long as it's HLL_8.
  hll_union_alloc<A> hllUnion(sk.get_lg_config_k());
  if (sk.get_target_type() == HLL_8) {    
    std::swap(hllUnion.gadget.hllSketchImpl, sk.hllSketchImpl);
  } else {
    hllUnion.update(sk);
  }
  return hllUnion;
}

template<typename A>
static std::ostream& operator<<(std::ostream& os, const hll_union_alloc<A>& hllUnion) {
  return hllUnion.to_string(os, true, true, false, false);
}

template<typename A>
hll_sketch_alloc<A> hll_union_alloc<A>::get_result(target_hll_type tgtHllType) const {
  return hll_sketch_alloc<A>(gadget, tgtHllType);
}

template<typename A>
void hll_union_alloc<A>::update(const hll_sketch_alloc<A>& sketch) {
  unionImpl(static_cast<const hll_sketch_alloc<A>&>(sketch).hllSketchImpl, lgMaxK);
}

template<typename A>
void hll_union_alloc<A>::update(const std::string& datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const uint64_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const uint32_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const uint16_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const uint8_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const int64_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const int32_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const int16_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const int8_t datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const double datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const float datum) {
  gadget.update(datum);
}

template<typename A>
void hll_union_alloc<A>::update(const void* data, const size_t lengthBytes) {
  gadget.update(data, lengthBytes);
}

template<typename A>
void hll_union_alloc<A>::couponUpdate(const int coupon) {
  if (coupon == HllUtil<A>::EMPTY) { return; }
  HllSketchImpl<A>* result = gadget.hllSketchImpl->couponUpdate(coupon);
  if (result != gadget.hllSketchImpl) {
    if (gadget.hllSketchImpl != nullptr) { gadget.hllSketchImpl->get_deleter()(gadget.hllSketchImpl); }
    gadget.hllSketchImpl = result;
  }
}

template<typename A>
std::pair<std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>, const size_t> hll_union_alloc<A>::serialize_compact() const {
  return gadget.serialize_compact();
}

template<typename A>
std::pair<std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>, const size_t> hll_union_alloc<A>::serialize_updatable() const {
  return gadget.serialize_updatable();
}

template<typename A>
void hll_union_alloc<A>::serialize_compact(std::ostream& os) const {
  return gadget.serialize_compact(os);
}

template<typename A>
void hll_union_alloc<A>::serialize_updatable(std::ostream& os) const {
  return gadget.serialize_updatable(os);
}

template<typename A>
std::ostream& hll_union_alloc<A>::to_string(std::ostream& os, const bool summary,
                                  const bool detail, const bool auxDetail, const bool all) const {
  return gadget.to_string(os, summary, detail, auxDetail, all);
}

template<typename A>
std::string hll_union_alloc<A>::to_string(const bool summary, const bool detail,
                                   const bool auxDetail, const bool all) const {
  return gadget.to_string(summary, detail, auxDetail, all);
}

template<typename A>
double hll_union_alloc<A>::get_estimate() const {
  return gadget.get_estimate();
}

template<typename A>
double hll_union_alloc<A>::get_composite_estimate() const {
  return gadget.get_composite_estimate();
}

template<typename A>
double hll_union_alloc<A>::get_lower_bound(const int numStdDev) const {
  return gadget.get_lower_bound(numStdDev);
}

template<typename A>
double hll_union_alloc<A>::get_upper_bound(const int numStdDev) const {
  return gadget.get_upper_bound(numStdDev);
}

template<typename A>
int hll_union_alloc<A>::get_compact_serialization_bytes() const {
  return gadget.get_compact_serialization_bytes();
}

template<typename A>
int hll_union_alloc<A>::get_updatable_serialization_bytes() const {
  return gadget.get_updatable_serialization_bytes();
}

template<typename A>
int hll_union_alloc<A>::get_lg_config_k() const {
  return gadget.get_lg_config_k();
}

template<typename A>
void hll_union_alloc<A>::reset() {
  gadget.reset();
}

template<typename A>
bool hll_union_alloc<A>::is_compact() const {
  return gadget.is_compact();
}

template<typename A>
bool hll_union_alloc<A>::is_empty() const {
  return gadget.is_empty();
}

template<typename A>
bool hll_union_alloc<A>::isOutOfOrderFlag() const {
  return gadget.isOutOfOrderFlag();
}

template<typename A>
CurMode hll_union_alloc<A>::getCurrentMode() const {
  return gadget.getCurrentMode();
}

template<typename A>
bool hll_union_alloc<A>::isEstimationMode() const {
  return gadget.isEstimationMode();
}

template<typename A>
int hll_union_alloc<A>::getSerializationVersion() const {
  return HllUtil<A>::SER_VER;
}

template<typename A>
target_hll_type hll_union_alloc<A>::get_target_type() const {
  return target_hll_type::HLL_8;
}

template<typename A>
int hll_union_alloc<A>::get_max_serialization_bytes(const int lgK) {
  return hll_sketch_alloc<A>::get_max_updatable_serialization_bytes(lgK, target_hll_type::HLL_8);
}

template<typename A>
double hll_union_alloc<A>::get_rel_err(const bool upperBound, const bool unioned,
                           const int lgConfigK, const int numStdDev) {
  return HllUtil<A>::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

template<typename A>
HllSketchImpl<A>* hll_union_alloc<A>::copyOrDownsampleHll(HllSketchImpl<A>* srcImpl, const int tgtLgK) {
  if (srcImpl->getCurMode() != CurMode::HLL) {
    throw std::logic_error("Attempt to downsample non-HLL sketch");
  }
  HllArray<A>* src = (HllArray<A>*) srcImpl;
  const int srcLgK = src->getLgConfigK();
  if ((srcLgK <= tgtLgK) && (src->getTgtHllType() == target_hll_type::HLL_8)) {
    return src->copy();
  }
  const int minLgK = ((srcLgK < tgtLgK) ? srcLgK : tgtLgK);
  HllArray<A>* tgtHllArr = HllSketchImplFactory<A>::newHll(minLgK, target_hll_type::HLL_8);
  pair_iterator_with_deleter<A> srcItr = src->getIterator();
  while (srcItr->nextValid()) {
    tgtHllArr->couponUpdate(srcItr->getPair());
  }
  //both of these are required for isomorphism
  tgtHllArr->putHipAccum(src->getHipAccum());
  tgtHllArr->putOutOfOrderFlag(src->isOutOfOrderFlag());
  
  return tgtHllArr;
}

template<typename A>
inline HllSketchImpl<A>* hll_union_alloc<A>::leakFreeCouponUpdate(HllSketchImpl<A>* impl, const int coupon) {
  HllSketchImpl<A>* result = impl->couponUpdate(coupon);
  if (result != impl) {
    impl->get_deleter()(impl);
  }
  return result;
}

template<typename A>
void hll_union_alloc<A>::unionImpl(HllSketchImpl<A>* incomingImpl, const int lgMaxK) {
  if (gadget.hllSketchImpl->getTgtHllType() != target_hll_type::HLL_8) {
    throw std::logic_error("Must call unionImpl() with HLL_8 input");
  }
  HllSketchImpl<A>* srcImpl = incomingImpl; //default
  HllSketchImpl<A>* dstImpl = gadget.hllSketchImpl; //default
  if ((incomingImpl == nullptr) || incomingImpl->isEmpty()) {
    return; // gadget.hllSketchImpl;
  }

  const int hi2bits = (gadget.hllSketchImpl->isEmpty()) ? 3 : gadget.hllSketchImpl->getCurMode();
  const int lo2bits = incomingImpl->getCurMode();

  const int sw = (hi2bits << 2) | lo2bits;
  //System.out.println("SW: " + sw);
  switch (sw) {
    case 0: { //src: LIST, gadget: LIST
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(dstImpl->isOutOfOrderFlag() | srcImpl->isOutOfOrderFlag());
      // gadget: cleanly updated as needed
      break;
    }
    case 1: { //src: SET, gadget: LIST
      //consider a swap here
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 2: { //src: HLL, gadget: LIST
      //swap so that src is gadget-LIST, tgt is HLL
      //use lgMaxK because LIST has effective K of 2^26
      srcImpl = gadget.hllSketchImpl;
      dstImpl = copyOrDownsampleHll(incomingImpl, lgMaxK);
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator();
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(srcImpl->isOutOfOrderFlag() | dstImpl->isOutOfOrderFlag());
      // gadget: swapped, replacing with new impl
      gadget.hllSketchImpl->get_deleter()(gadget.hllSketchImpl);
      break;
    }
    case 4: { //src: LIST, gadget: SET
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 5: { //src: SET, gadget: SET
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 6: { //src: HLL, gadget: SET
      //swap so that src is gadget-SET, tgt is HLL
      //use lgMaxK because LIST has effective K of 2^26
      srcImpl = gadget.hllSketchImpl;
      dstImpl = copyOrDownsampleHll(incomingImpl, lgMaxK);
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //LIST
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into non-empty HLL -> true
      // gadget: swapped, replacing with new impl
      gadget.hllSketchImpl->get_deleter()(gadget.hllSketchImpl);
      break;
    }
    case 8: { //src: LIST, gadget: HLL
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(dstImpl->isOutOfOrderFlag() | srcImpl->isOutOfOrderFlag());
      // gadget: should remain unchanged
      if (dstImpl != gadget.hllSketchImpl) {
        // should not have changed from HLL
        throw std::logic_error("dstImpl unepxectedly changed from gadget");
      } 
      break;
    }
    case 9: { //src: SET, gadget: HLL
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into existing HLL -> true
      // gadget: should remain unchanged
      if (dstImpl != gadget.hllSketchImpl) {
        // should not have changed from HLL
        throw std::logic_error("dstImpl unepxectedly changed from gadget");
      } 
      break;
    }
    case 10: { //src: HLL, gadget: HLL
      const int srcLgK = srcImpl->getLgConfigK();
      const int dstLgK = dstImpl->getLgConfigK();
      const int minLgK = ((srcLgK < dstLgK) ? srcLgK : dstLgK);
      if ((srcLgK < dstLgK) || (dstImpl->getTgtHllType() != HLL_8)) {
        dstImpl = copyOrDownsampleHll(dstImpl, minLgK);
        // always replaces gadget
        gadget.hllSketchImpl->get_deleter()(gadget.hllSketchImpl);
      }
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //HLL
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //union of two HLL modes is always true
      // gadget: replaced if copied/downampled, otherwise should be unchanged
      break;
    }
    case 12: { //src: LIST, gadget: empty
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(srcImpl->isOutOfOrderFlag()); //whatever source is
      // gadget: cleanly updated as needed
      break;
    }
    case 13: { //src: SET, gadget: empty
      pair_iterator_with_deleter<A> srcItr = srcImpl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leakFreeCouponUpdate(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 14: { //src: HLL, gadget: empty
      dstImpl = copyOrDownsampleHll(srcImpl, lgMaxK);
      dstImpl->putOutOfOrderFlag(srcImpl->isOutOfOrderFlag()); //whatever source is.
      // gadget: always replaced with copied/downsampled sketch
      gadget.hllSketchImpl->get_deleter()(gadget.hllSketchImpl);
      break;
    }
  }
  
  gadget.hllSketchImpl = dstImpl;
}

}

#endif // _HLLUNION_INTERNAL_HPP_
