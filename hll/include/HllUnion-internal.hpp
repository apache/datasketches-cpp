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
hll_union_alloc<A>::hll_union_alloc(const int lg_max_k):
  lg_max_k(HllUtil<A>::checkLgK(lg_max_k)),
  gadget(lg_max_k, target_hll_type::HLL_8)
{}

template<typename A>
hll_union_alloc<A> hll_union_alloc<A>::deserialize(const void* bytes, size_t len) {
  hll_sketch_alloc<A> sk(hll_sketch_alloc<A>::deserialize(bytes, len));
  // we're using the sketch's lg_config_k to initialize the union so
  // we can initialize the Union with it as long as it's HLL_8.
  hll_union_alloc<A> hllUnion(sk.get_lg_config_k());
  if (sk.get_target_type() == HLL_8) {
    std::swap(hllUnion.gadget.sketch_impl, sk.sketch_impl);
  } else {
    hllUnion.update(sk);
  }
  return hllUnion;
}

template<typename A>
hll_union_alloc<A> hll_union_alloc<A>::deserialize(std::istream& is) {
  hll_sketch_alloc<A> sk(hll_sketch_alloc<A>::deserialize(is));
  // we're using the sketch's lg_config_k to initialize the union so
  // we can initialize the Union with it as long as it's HLL_8.
  hll_union_alloc<A> hllUnion(sk.get_lg_config_k());
  if (sk.get_target_type() == HLL_8) {    
    std::swap(hllUnion.gadget.sketch_impl, sk.sketch_impl);
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
hll_sketch_alloc<A> hll_union_alloc<A>::get_result(target_hll_type target_type) const {
  return hll_sketch_alloc<A>(gadget, target_type);
}

template<typename A>
void hll_union_alloc<A>::update(const hll_sketch_alloc<A>& sketch) {
  union_impl(static_cast<const hll_sketch_alloc<A>&>(sketch).sketch_impl, lg_max_k);
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
void hll_union_alloc<A>::update(const void* data, const size_t length_bytes) {
  gadget.update(data, length_bytes);
}

template<typename A>
void hll_union_alloc<A>::coupon_update(const int coupon) {
  if (coupon == HllUtil<A>::EMPTY) { return; }
  HllSketchImpl<A>* result = gadget.sketch_impl->coupon_update(coupon);
  if (result != gadget.sketch_impl) {
    if (gadget.sketch_impl != nullptr) { gadget.sketch_impl->get_deleter()(gadget.sketch_impl); }
    gadget.sketch_impl = result;
  }
}

template<typename A>
vector_u8<A> hll_union_alloc<A>::serialize_compact() const {
  return gadget.serialize_compact();
}

template<typename A>
vector_u8<A> hll_union_alloc<A>::serialize_updatable() const {
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
                                  const bool detail, const bool aux_detail, const bool all) const {
  return gadget.to_string(os, summary, detail, aux_detail, all);
}

template<typename A>
std::string hll_union_alloc<A>::to_string(const bool summary, const bool detail,
                                   const bool aux_detail, const bool all) const {
  return gadget.to_string(summary, detail, aux_detail, all);
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
double hll_union_alloc<A>::get_lower_bound(const int num_std_dev) const {
  return gadget.get_lower_bound(num_std_dev);
}

template<typename A>
double hll_union_alloc<A>::get_upper_bound(const int num_std_dev) const {
  return gadget.get_upper_bound(num_std_dev);
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
bool hll_union_alloc<A>::is_out_of_order_flag() const {
  return gadget.is_out_of_order_flag();
}

template<typename A>
hll_mode hll_union_alloc<A>::get_current_mode() const {
  return gadget.get_current_mode();
}

template<typename A>
bool hll_union_alloc<A>::is_estimation_mode() const {
  return gadget.is_estimation_mode();
}

template<typename A>
int hll_union_alloc<A>::get_serialization_version() const {
  return HllUtil<A>::SER_VER;
}

template<typename A>
target_hll_type hll_union_alloc<A>::get_target_type() const {
  return target_hll_type::HLL_8;
}

template<typename A>
int hll_union_alloc<A>::get_max_serialization_bytes(const int lg_k) {
  return hll_sketch_alloc<A>::get_max_updatable_serialization_bytes(lg_k, target_hll_type::HLL_8);
}

template<typename A>
double hll_union_alloc<A>::get_rel_err(const bool upper_bound, const bool unioned,
                           const int lg_config_k, const int num_std_dev) {
  return HllUtil<A>::getRelErr(upper_bound, unioned, lg_config_k, num_std_dev);
}

template<typename A>
HllSketchImpl<A>* hll_union_alloc<A>::copy_or_downsample(HllSketchImpl<A>* src_impl, const int tgt_lg_k) {
  if (src_impl->getCurMode() != HLL) {
    throw std::logic_error("Attempt to downsample non-HLL sketch");
  }
  HllArray<A>* src = (HllArray<A>*) src_impl;
  const int src_lg_k = src->getLgConfigK();
  if ((src_lg_k <= tgt_lg_k) && (src->getTgtHllType() == target_hll_type::HLL_8)) {
    return src->copy();
  }
  const int minLgK = ((src_lg_k < tgt_lg_k) ? src_lg_k : tgt_lg_k);
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
inline HllSketchImpl<A>* hll_union_alloc<A>::leak_free_coupon_update(HllSketchImpl<A>* impl, const int coupon) {
  HllSketchImpl<A>* result = impl->couponUpdate(coupon);
  if (result != impl) {
    impl->get_deleter()(impl);
  }
  return result;
}

template<typename A>
void hll_union_alloc<A>::union_impl(HllSketchImpl<A>* incoming_impl, const int lg_max_k) {
  if (gadget.sketch_impl->getTgtHllType() != target_hll_type::HLL_8) {
    throw std::logic_error("Must call unionImpl() with HLL_8 input");
  }
  HllSketchImpl<A>* src_impl = incoming_impl; //default
  HllSketchImpl<A>* dstImpl = gadget.sketch_impl; //default
  if ((incoming_impl == nullptr) || incoming_impl->isEmpty()) {
    return; // gadget.sketch_impl;
  }

  const int hi2bits = (gadget.sketch_impl->isEmpty()) ? 3 : gadget.sketch_impl->getCurMode();
  const int lo2bits = incoming_impl->getCurMode();

  const int sw = (hi2bits << 2) | lo2bits;
  switch (sw) {
    case 0: { //src: LIST, gadget: LIST
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(dstImpl->isOutOfOrderFlag() | src_impl->isOutOfOrderFlag());
      // gadget: cleanly updated as needed
      break;
    }
    case 1: { //src: SET, gadget: LIST
      //consider a swap here
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 2: { //src: HLL, gadget: LIST
      //swap so that src is gadget-LIST, tgt is HLL
      //use lg_max_k because LIST has effective K of 2^26
      src_impl = gadget.sketch_impl;
      dstImpl = copy_or_downsample(incoming_impl, lg_max_k);
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator();
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(src_impl->isOutOfOrderFlag() | dstImpl->isOutOfOrderFlag());
      // gadget: swapped, replacing with new impl
      gadget.sketch_impl->get_deleter()(gadget.sketch_impl);
      break;
    }
    case 4: { //src: LIST, gadget: SET
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 5: { //src: SET, gadget: SET
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 6: { //src: HLL, gadget: SET
      //swap so that src is gadget-SET, tgt is HLL
      //use lg_max_k because LIST has effective K of 2^26
      src_impl = gadget.sketch_impl;
      dstImpl = copy_or_downsample(incoming_impl, lg_max_k);
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //LIST
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into non-empty HLL -> true
      // gadget: swapped, replacing with new impl
      gadget.sketch_impl->get_deleter()(gadget.sketch_impl);
      break;
    }
    case 8: { //src: LIST, gadget: HLL
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      //whichever is True wins:
      dstImpl->putOutOfOrderFlag(dstImpl->isOutOfOrderFlag() | src_impl->isOutOfOrderFlag());
      // gadget: should remain unchanged
      if (dstImpl != gadget.sketch_impl) {
        // should not have changed from HLL
        throw std::logic_error("dstImpl unepxectedly changed from gadget");
      } 
      break;
    }
    case 9: { //src: SET, gadget: HLL
      if (dstImpl->getCurMode() != HLL) {
        throw std::logic_error("dstImpl must be in HLL mode");
      }
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //merging SET into existing HLL -> true
      // gadget: should remain unchanged
      if (dstImpl != gadget.sketch_impl) {
        // should not have changed from HLL
        throw std::logic_error("dstImpl unepxectedly changed from gadget");
      } 
      break;
    }
    case 10: { //src: HLL, gadget: HLL
      const int src_lg_k = src_impl->getLgConfigK();
      const int dstLgK = dstImpl->getLgConfigK();
      const int minLgK = ((src_lg_k < dstLgK) ? src_lg_k : dstLgK);
      if ((src_lg_k < dstLgK) || (dstImpl->getTgtHllType() != HLL_8)) {
        dstImpl = copy_or_downsample(dstImpl, minLgK);
        // always replaces gadget
        gadget.sketch_impl->get_deleter()(gadget.sketch_impl);
      }
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //HLL
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //union of two HLL modes is always true
      // gadget: replaced if copied/downampled, otherwise should be unchanged
      break;
    }
    case 12: { //src: LIST, gadget: empty
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //LIST
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(src_impl->isOutOfOrderFlag()); //whatever source is
      // gadget: cleanly updated as needed
      break;
    }
    case 13: { //src: SET, gadget: empty
      pair_iterator_with_deleter<A> srcItr = src_impl->getIterator(); //SET
      while (srcItr->nextValid()) {
        dstImpl = leak_free_coupon_update(dstImpl, srcItr->getPair()); //assignment required
      }
      dstImpl->putOutOfOrderFlag(true); //SET oooFlag is always true
      // gadget: cleanly updated as needed
      break;
    }
    case 14: { //src: HLL, gadget: empty
      dstImpl = copy_or_downsample(src_impl, lg_max_k);
      dstImpl->putOutOfOrderFlag(src_impl->isOutOfOrderFlag()); //whatever source is.
      // gadget: always replaced with copied/downsampled sketch
      gadget.sketch_impl->get_deleter()(gadget.sketch_impl);
      break;
    }
  }
  
  gadget.sketch_impl = dstImpl;
}

}

#endif // _HLLUNION_INTERNAL_HPP_
