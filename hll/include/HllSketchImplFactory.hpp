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

#ifndef _HLLSKETCHIMPLFACTORY_HPP_
#define _HLLSKETCHIMPLFACTORY_HPP_

#include <functional>
#include <memory>
#include <stdexcept>

#include "HllUtil.hpp"
#include "HllSketchImpl.hpp"
#include "CouponList.hpp"
#include "CouponHashSet.hpp"
#include "HllArray.hpp"
#include "Hll4Array.hpp"
#include "Hll6Array.hpp"
#include "Hll8Array.hpp"

namespace datasketches {

template<typename A>
class HllSketchImplFactory final {
public:
  static HllSketchImpl<A>* deserialize(std::istream& os, const A& allocator);
  static HllSketchImpl<A>* deserialize(const void* bytes, size_t len, const A& allocator);

  static CouponHashSet<A>* promoteListToSet(const CouponList<A>& list);
  static HllArray<A>* promoteListOrSetToHll(const CouponList<A>& list);
  static HllArray<A>* newHll(uint8_t lgConfigK, target_hll_type tgtHllType, const A& allocator);
  
  // resets the input impl, deleting the input pointer and returning a new pointer
  // full_size selects the state to reset to: an empty HLL array, or LIST (coupon) mode
  static HllSketchImpl<A>* reset(HllSketchImpl<A>* impl, bool full_size);

  static Hll4Array<A>* convertToHll4(const HllArray<A>& srcHllArr);
  static Hll6Array<A>* convertToHll6(const HllArray<A>& srcHllArr);
  static Hll8Array<A>* convertToHll8(const HllArray<A>& srcHllArr);
};

template<typename A>
CouponHashSet<A>* HllSketchImplFactory<A>::promoteListToSet(const CouponList<A>& list) {
  auto chSet = HllUtil<A>::template newObject<CouponHashSet<A>>(list.getAllocator(),
      list.getLgConfigK(), list.getTgtHllType(), list.getAllocator());
  std::unique_ptr<CouponHashSet<A>, std::function<void(HllSketchImpl<A>*)>> ptr(chSet, chSet->get_deleter());
  for (const auto coupon: list) {
    chSet->couponUpdate(coupon);
  }
  return ptr.release();
}

template<typename A>
HllArray<A>* HllSketchImplFactory<A>::promoteListOrSetToHll(const CouponList<A>& src) {
  HllArray<A>* tgtHllArr = HllSketchImplFactory<A>::newHll(src.getLgConfigK(), src.getTgtHllType(), src.getAllocator());
  std::unique_ptr<HllArray<A>, std::function<void(HllSketchImpl<A>*)>> ptr(tgtHllArr, tgtHllArr->get_deleter());
  tgtHllArr->putKxQ0(1 << src.getLgConfigK());
  for (const auto coupon: src) {
    tgtHllArr->couponUpdate(coupon);
  }
  tgtHllArr->putHipAccum(src.getEstimate());
  tgtHllArr->putOutOfOrderFlag(false);
  return ptr.release();
}

template<typename A>
HllSketchImpl<A>* HllSketchImplFactory<A>::deserialize(std::istream& is, const A& allocator) {
  // we'll hand off the sketch based on PreInts so we don't need
  // to move the stream pointer back and forth -- perhaps somewhat fragile?
  const uint8_t preInts = static_cast<uint8_t>(is.peek());
  if (preInts == hll_constants::HLL_PREINTS) {
    return HllArray<A>::newHll(is, allocator);
  } else if (preInts == hll_constants::HASH_SET_PREINTS) {
    return CouponHashSet<A>::newSet(is, allocator);
  } else if (preInts == hll_constants::LIST_PREINTS) {
    return CouponList<A>::newList(is, allocator);
  } else {
    throw std::invalid_argument("Attempt to deserialize unknown object type");
  }
}

template<typename A>
HllSketchImpl<A>* HllSketchImplFactory<A>::deserialize(const void* bytes, size_t len, const A& allocator) {
  // read current mode directly
  const uint8_t preInts = static_cast<const uint8_t*>(bytes)[0];
  if (preInts == hll_constants::HLL_PREINTS) {
    return HllArray<A>::newHll(bytes, len, allocator);
  } else if (preInts == hll_constants::HASH_SET_PREINTS) {
    return CouponHashSet<A>::newSet(bytes, len, allocator);
  } else if (preInts == hll_constants::LIST_PREINTS) {
    return CouponList<A>::newList(bytes, len, allocator);
  } else {
    throw std::invalid_argument("Attempt to deserialize unknown object type");
  }
}

template<typename A>
HllArray<A>* HllSketchImplFactory<A>::newHll(uint8_t lgConfigK, target_hll_type tgtHllType, const A& allocator) {
  switch (tgtHllType) {
    case HLL_8:
      return HllUtil<A>::template newObject<Hll8Array<A>>(allocator, lgConfigK, allocator);
    case HLL_6:
      return HllUtil<A>::template newObject<Hll6Array<A>>(allocator, lgConfigK, allocator);
    case HLL_4:
      return HllUtil<A>::template newObject<Hll4Array<A>>(allocator, lgConfigK, allocator);
  }
  throw std::logic_error("Invalid target_hll_type");
}

template<typename A>
HllSketchImpl<A>* HllSketchImplFactory<A>::reset(HllSketchImpl<A>* impl, bool full_size) {
  if (full_size) {
    HllArray<A>* hll = newHll(impl->getLgConfigK(), impl->getTgtHllType(), impl->getAllocator());
    impl->get_deleter()(impl);
    return hll;
  } else {
    auto cl = HllUtil<A>::template newObject<CouponList<A>>(impl->getAllocator(),
        impl->getLgConfigK(), impl->getTgtHllType(), hll_mode::LIST, impl->getAllocator());
    impl->get_deleter()(impl);
    return cl;
  }
}

template<typename A>
Hll4Array<A>* HllSketchImplFactory<A>::convertToHll4(const HllArray<A>& srcHllArr) {
  return HllUtil<A>::template newObject<Hll4Array<A>>(srcHllArr.getAllocator(), srcHllArr);
}

template<typename A>
Hll6Array<A>* HllSketchImplFactory<A>::convertToHll6(const HllArray<A>& srcHllArr) {
  return HllUtil<A>::template newObject<Hll6Array<A>>(srcHllArr.getAllocator(), srcHllArr);
}

template<typename A>
Hll8Array<A>* HllSketchImplFactory<A>::convertToHll8(const HllArray<A>& srcHllArr) {
  return HllUtil<A>::template newObject<Hll8Array<A>>(srcHllArr.getAllocator(), srcHllArr);
}

}

#endif /* _HLLSKETCHIMPLFACTORY_HPP_ */
