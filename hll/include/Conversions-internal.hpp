/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _CONVERSIONS_INTERNAL_HPP_
#define _CONVERSIONS_INTERNAL_HPP_

#include "Conversions.hpp"
#include "HllUtil.hpp"
#include "HllArray.hpp"

#include <memory>

namespace datasketches {

Hll4Array* Conversions::convertToHll4(const HllArray& srcHllArr) {
  const int lgConfigK = srcHllArr.getLgConfigK();
  Hll4Array* hll4Array = new Hll4Array(lgConfigK);
  hll4Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  // 1st pass: compute starting curMin and numAtCurMin
  int pairVals = curMinAndNum(srcHllArr);
  int curMin = HllUtil::getValue(pairVals);
  int numAtCurMin = HllUtil::getLow26(pairVals);

  // 2nd pass: must know curMin.
  // Populate KxQ registers, build AuxHashMap if needed
  std::unique_ptr<PairIterator> itr = srcHllArr.getIterator();
  // nothing allocated, may be null
  AuxHashMap* auxHashMap = srcHllArr.getAuxHashMap();

  while (itr->nextValid()) {
    const int slotNo = itr->getIndex();
    const int actualValue = itr->getValue();
    HllArray::hipAndKxQIncrementalUpdate(*hll4Array, 0, actualValue);
    if (actualValue >= (curMin + 15)) {
      hll4Array->putSlot(slotNo, HllUtil::AUX_TOKEN);
      if (auxHashMap == nullptr) {
        auxHashMap = new AuxHashMap(HllUtil::LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
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

int Conversions::curMinAndNum(const HllArray& hllArr) {
  int curMin = 64;
  int numAtCurMin = 0;
  std::unique_ptr<PairIterator> itr = hllArr.getIterator();
  while (itr->nextAll()) {
    int v = itr->getValue();
    if (v < curMin) {
      curMin = v;
      numAtCurMin = 1;
    } else if (v == curMin) {
      ++numAtCurMin;
    }
  }

  return HllUtil::pair(numAtCurMin, curMin);
}

Hll6Array* Conversions::convertToHll6(const HllArray& srcHllArr) {
  const int lgConfigK = srcHllArr.getLgConfigK();
  Hll6Array* hll6Array = new Hll6Array(lgConfigK);
  hll6Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  int numZeros = 1 << lgConfigK;
  std::unique_ptr<PairIterator> itr = srcHllArr.getIterator();
  while (itr->nextAll()) {
    if (itr->getValue() != HllUtil::EMPTY) {
      --numZeros;
      hll6Array->couponUpdate(itr->getPair());
    }
  }

  hll6Array->putNumAtCurMin(numZeros);
  hll6Array->putHipAccum(srcHllArr.getHipAccum());
  return hll6Array;
}

Hll8Array* Conversions::convertToHll8(const HllArray& srcHllArr) {
  const int lgConfigK = srcHllArr.getLgConfigK();
  Hll8Array* hll8Array = new Hll8Array(lgConfigK);
  hll8Array->putOutOfOrderFlag(srcHllArr.isOutOfOrderFlag());

  int numZeros = 1 << lgConfigK;
  std::unique_ptr<PairIterator> itr = srcHllArr.getIterator();
  while (itr->nextAll()) {
    if (itr->getValue() != HllUtil::EMPTY) {
      --numZeros;
      hll8Array->couponUpdate(itr->getPair());
    }
  }

  hll8Array->putNumAtCurMin(numZeros);
  hll8Array->putHipAccum(srcHllArr.getHipAccum());
  return hll8Array;
}

}

#endif // _CONVERSIONS_INTERNAL_HPP_