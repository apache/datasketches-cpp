/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
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
  static HllArray<A>* newHll(int lgConfigK, TgtHllType tgtHllType);
  
  static HllSketchImpl<A>* reset(const HllSketchImpl<A>* impl);

  static Hll4Array<A>* convertToHll4(const HllArray<A>& srcHllArr);
  static Hll6Array<A>* convertToHll6(const HllArray<A>& srcHllArr);
  static Hll8Array<A>* convertToHll8(const HllArray<A>& srcHllArr);

private:
  static int curMinAndNum(const HllArray<A>& hllArr);
};

template<typename A>
CouponHashSet<A>* HllSketchImplFactory<A>::promoteListToSet(const CouponList<A>& list) {
  std::unique_ptr<PairIterator<A>> iter = list.getIterator();
  CouponHashSet<A>* chSet = new CouponHashSet<A>(list.getLgConfigK(), list.getTgtHllType());
  while (iter->nextValid()) {
    chSet->couponUpdate(iter->getPair());
  }
  chSet->putOutOfOrderFlag(true);

  return chSet;
}

template<typename A>
HllArray<A>* HllSketchImplFactory<A>::promoteListOrSetToHll(const CouponList<A>& src) {
  HllArray<A>* tgtHllArr = HllSketchImplFactory<A>::newHll(src.getLgConfigK(), src.getTgtHllType());
  std::unique_ptr<PairIterator<A>> srcItr = src.getIterator();
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
HllArray<A>* HllSketchImplFactory<A>::newHll(int lgConfigK, TgtHllType tgtHllType) {
  switch (tgtHllType) {
    case HLL_8:
      return (HllArray<A>*) new Hll8Array<A>(lgConfigK);
    case HLL_6:
      return (HllArray<A>*) new Hll6Array<A>(lgConfigK);
    case HLL_4:
      return (HllArray<A>*) new Hll4Array<A>(lgConfigK);
  }
  throw std::logic_error("Invalid TgtHllType");
}

template<typename A>
HllSketchImpl<A>* HllSketchImplFactory<A>::reset(const HllSketchImpl<A>* impl) {
  return new CouponList<A>(impl->getLgConfigK(), impl->getTgtHllType(), CurMode::LIST);
}


template<typename A>
Hll4Array<A>* HllSketchImplFactory<A>::convertToHll4(const HllArray<A>& srcHllArr) {
  const int lgConfigK = srcHllArr.getLgConfigK();
  Hll4Array<A>* hll4Array = new Hll4Array<A>(lgConfigK);
  hll4Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  // 1st pass: compute starting curMin and numAtCurMin
  int pairVals = curMinAndNum(srcHllArr);
  int curMin = HllUtil<A>::getValue(pairVals);
  int numAtCurMin = HllUtil<A>::getLow26(pairVals);

  // 2nd pass: must know curMin.
  // Populate KxQ registers, build AuxHashMap if needed
  std::unique_ptr<PairIterator<A>> itr = srcHllArr.getIterator();
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
  std::unique_ptr<PairIterator<A>> itr = hllArr.getIterator();
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
  Hll6Array<A>* hll6Array = new Hll6Array<A>(lgConfigK);
  hll6Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  int numZeros = 1 << lgConfigK;
  std::unique_ptr<PairIterator<A>> itr = srcHllArr.getIterator();
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
  Hll8Array<A>* hll8Array = new Hll8Array<A>(lgConfigK);
  hll8Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  int numZeros = 1 << lgConfigK;
  std::unique_ptr<PairIterator<A>> itr = srcHllArr.getIterator();
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

//#include "HllSketchImpl-internal.hpp"
//#include "CouponList-internal.hpp"
//#include "CouponHashSet-internal.hpp"
//#include "HllArray-internal.hpp"
//#include "Hll4Array-internal.hpp"
//#include "Hll6Array-internal.hpp"
//#include "Hll8Array-internal.hpp"

#endif /* _HLLSKETCHIMPLFACTORY_HPP_ */