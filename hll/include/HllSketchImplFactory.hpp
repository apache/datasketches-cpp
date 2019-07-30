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

#include "HllUtil.hpp"
#include "HllSketchImpl.hpp"
#include "CouponList.hpp"
#include "CouponHashSet.hpp"
#include "HllArray.hpp"
#include "Hll4Array.hpp"
#include "Hll6Array.hpp"
#include "Hll8Array.hpp"

namespace datasketches {

template<typename A = std::allocator<char>>
class HllSketchImplFactory final {
public:
  static HllSketchImpl<A>* deserialize(std::istream& os);
  static HllSketchImpl<A>* deserialize(const void* bytes, size_t len);

  static CouponHashSet<A>* promoteListToSet(const CouponList<A>& list);
  static HllArray<A>* promoteListOrSetToHll(const CouponList<A>& list);
  static HllArray<A>* newHll(int lgConfigK, TgtHllType tgtHllType, bool startFullSize = false);
  
  // resets the input impl, deleting the input pointert and returning a new pointer
  static HllSketchImpl<A>* reset(HllSketchImpl<A>* impl, bool startFullSize);

  static Hll4Array<A>* convertToHll4(const HllArray<A>& srcHllArr);
  static Hll6Array<A>* convertToHll6(const HllArray<A>& srcHllArr);
  static Hll8Array<A>* convertToHll8(const HllArray<A>& srcHllArr);

private:
  static int curMinAndNum(const HllArray<A>& hllArr);
};

template<typename A>
CouponHashSet<A>* HllSketchImplFactory<A>::promoteListToSet(const CouponList<A>& list) {
  PairIterator_with_deleter<A> iter = list.getIterator();

  typedef typename std::allocator_traits<A>::template rebind_alloc<CouponHashSet<A>> chsAlloc;
  CouponHashSet<A>* chSet = new (chsAlloc().allocate(1)) CouponHashSet<A>(list.getLgConfigK(), list.getTgtHllType());
  while (iter->nextValid()) {
    chSet->couponUpdate(iter->getPair());
  }
  chSet->putOutOfOrderFlag(true);

  return chSet;
}

template<typename A>
HllArray<A>* HllSketchImplFactory<A>::promoteListOrSetToHll(const CouponList<A>& src) {
  HllArray<A>* tgtHllArr = HllSketchImplFactory<A>::newHll(src.getLgConfigK(), src.getTgtHllType());
  PairIterator_with_deleter<A> srcItr = src.getIterator();
  tgtHllArr->putKxQ0(1 << src.getLgConfigK());
  while (srcItr->nextValid()) {
    tgtHllArr->couponUpdate(srcItr->getPair());
    tgtHllArr->putHipAccum(src.getEstimate());
  }
  tgtHllArr->putOutOfOrderFlag(false);
  return tgtHllArr;
}

template<typename A>
HllSketchImpl<A>* HllSketchImplFactory<A>::deserialize(std::istream& is) {
  // we'll hand off the sketch based on PreInts so we don't need
  // to move the stream pointer back and forth -- perhaps somewhat fragile?
  const int preInts = is.peek();
  if (preInts == HllUtil<A>::HLL_PREINTS) {
    return HllArray<A>::newHll(is);
  } else if (preInts == HllUtil<A>::HASH_SET_PREINTS) {
    return CouponHashSet<A>::newSet(is);
  } else if (preInts == HllUtil<A>::LIST_PREINTS) {
    return CouponList<A>::newList(is);
  } else {
    throw std::invalid_argument("Attempt to deserialize unknown object type");
  }
}

template<typename A>
HllSketchImpl<A>* HllSketchImplFactory<A>::deserialize(const void* bytes, size_t len) {
  // read current mode directly
  const int preInts = static_cast<const uint8_t*>(bytes)[0];
  if (preInts == HllUtil<A>::HLL_PREINTS) {
    return HllArray<A>::newHll(bytes, len);
  } else if (preInts == HllUtil<A>::HASH_SET_PREINTS) {
    return CouponHashSet<A>::newSet(bytes, len);
  } else if (preInts == HllUtil<A>::LIST_PREINTS) {
    return CouponList<A>::newList(bytes, len);
  } else {
    throw std::invalid_argument("Attempt to deserialize unknown object type");
  }
}

template<typename A>
HllArray<A>* HllSketchImplFactory<A>::newHll(int lgConfigK, TgtHllType tgtHllType, bool startFullSize) {
  switch (tgtHllType) {
    case HLL_8:
      typedef typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>> hll8Alloc;
      return new (hll8Alloc().allocate(1)) Hll8Array<A>(lgConfigK, startFullSize);
    case HLL_6:
      typedef typename std::allocator_traits<A>::template rebind_alloc<Hll6Array<A>> hll6Alloc;
      return new (hll6Alloc().allocate(1)) Hll6Array<A>(lgConfigK, startFullSize);
    case HLL_4:
      typedef typename std::allocator_traits<A>::template rebind_alloc<Hll4Array<A>> hll4Alloc;
      return new (hll4Alloc().allocate(1)) Hll4Array<A>(lgConfigK, startFullSize);
  }
  throw std::logic_error("Invalid TgtHllType");
}

template<typename A>
HllSketchImpl<A>* HllSketchImplFactory<A>::reset(HllSketchImpl<A>* impl, bool startFullSize) {
  if (startFullSize) {
    HllArray<A>* hll = newHll(impl->getLgConfigK(), impl->getTgtHllType(), startFullSize);
    impl->get_deleter()(impl);
    return hll;
  } else {
    typedef typename std::allocator_traits<A>::template rebind_alloc<CouponList<A>> clAlloc;
    CouponList<A>* cl = new (clAlloc().allocate(1)) CouponList<A>(impl->getLgConfigK(), impl->getTgtHllType(), CurMode::LIST);
    impl->get_deleter()(impl);
    return cl;
  }
}

template<typename A>
Hll4Array<A>* HllSketchImplFactory<A>::convertToHll4(const HllArray<A>& srcHllArr) {
  const int lgConfigK = srcHllArr.getLgConfigK();
  typedef typename std::allocator_traits<A>::template rebind_alloc<Hll4Array<A>> hll4Alloc;
  Hll4Array<A>* hll4Array = new (hll4Alloc().allocate(1)) Hll4Array<A>(lgConfigK, srcHllArr.isStartFullSize());
  hll4Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  // 1st pass: compute starting curMin and numAtCurMin
  int pairVals = curMinAndNum(srcHllArr);
  int curMin = HllUtil<A>::getValue(pairVals);
  int numAtCurMin = HllUtil<A>::getLow26(pairVals);

  // 2nd pass: must know curMin.
  // Populate KxQ registers, build AuxHashMap if needed
  PairIterator_with_deleter<A> itr = srcHllArr.getIterator();
  // nothing allocated, may be null
  AuxHashMap<A>* auxHashMap = srcHllArr.getAuxHashMap();

  while (itr->nextValid()) {
    const int slotNo = itr->getIndex();
    const int actualValue = itr->getValue();
    HllArray<A>::hipAndKxQIncrementalUpdate(*hll4Array, 0, actualValue);
    if (actualValue >= (curMin + 15)) {
      hll4Array->putSlot(slotNo, HllUtil<A>::AUX_TOKEN);
      if (auxHashMap == nullptr) {
        auxHashMap = AuxHashMap<A>::newAuxHashMap(HllUtil<A>::LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
        hll4Array->putAuxHashMap(auxHashMap);
      }
      auxHashMap->mustAdd(slotNo, actualValue);
    } else {
      hll4Array->putSlot(slotNo, actualValue - curMin);
    }
  }

  hll4Array->putCurMin(curMin);
  hll4Array->putNumAtCurMin(numAtCurMin);
  hll4Array->putHipAccum(srcHllArr.getHipAccum());

  return hll4Array;
}

template<typename A>
int HllSketchImplFactory<A>::curMinAndNum(const HllArray<A>& hllArr) {
  int curMin = 64;
  int numAtCurMin = 0;
  PairIterator_with_deleter<A> itr = hllArr.getIterator();
  while (itr->nextAll()) {
    int v = itr->getValue();
    if (v < curMin) {
      curMin = v;
      numAtCurMin = 1;
    } else if (v == curMin) {
      ++numAtCurMin;
    }
  }

  return HllUtil<A>::pair(numAtCurMin, curMin);
}

template<typename A>
Hll6Array<A>* HllSketchImplFactory<A>::convertToHll6(const HllArray<A>& srcHllArr) {
  const int lgConfigK = srcHllArr.getLgConfigK();
  typedef typename std::allocator_traits<A>::template rebind_alloc<Hll6Array<A>> hll6Alloc;
  Hll6Array<A>* hll6Array = new (hll6Alloc().allocate(1)) Hll6Array<A>(lgConfigK, srcHllArr.isStartFullSize());
  hll6Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  int numZeros = 1 << lgConfigK;
  PairIterator_with_deleter<A> itr = srcHllArr.getIterator();
  while (itr->nextAll()) {
    if (itr->getValue() != HllUtil<A>::EMPTY) {
      --numZeros;
      hll6Array->couponUpdate(itr->getPair());
    }
  }

  hll6Array->putNumAtCurMin(numZeros);
  hll6Array->putHipAccum(srcHllArr.getHipAccum());
  return hll6Array;
}

template<typename A>
Hll8Array<A>* HllSketchImplFactory<A>::convertToHll8(const HllArray<A>& srcHllArr) {
  const int lgConfigK = srcHllArr.getLgConfigK();
  typedef typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>> hll8Alloc;
  Hll8Array<A>* hll8Array = new (hll8Alloc().allocate(1)) Hll8Array<A>(lgConfigK, srcHllArr.isStartFullSize());
  hll8Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  int numZeros = 1 << lgConfigK;
  PairIterator_with_deleter<A> itr = srcHllArr.getIterator();
  while (itr->nextAll()) {
    if (itr->getValue() != HllUtil<A>::EMPTY) {
      --numZeros;
      hll8Array->couponUpdate(itr->getPair());
    }
  }

  hll8Array->putNumAtCurMin(numZeros);
  hll8Array->putHipAccum(srcHllArr.getHipAccum());
  return hll8Array;
}

}

#endif /* _HLLSKETCHIMPLFACTORY_HPP_ */
