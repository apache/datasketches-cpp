/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cstring>

#include "Hll8Array.hpp"

namespace datasketches {

Hll8Iterator::Hll8Iterator(const Hll8Array& hllArray, const int lengthPairs)
  : HllPairIterator(lengthPairs),
    hllArray(hllArray)
{}

Hll8Iterator::~Hll8Iterator() { }

int Hll8Iterator::value() {
  return hllArray.hllByteArr[index] & HllUtil::VAL_MASK_6;
}

Hll8Array::Hll8Array(const int lgConfigK) :
    HllArray(lgConfigK, TgtHllType::HLL_8) {
  const int numBytes = hll8ArrBytes(lgConfigK);
  hllByteArr = new uint8_t[numBytes];
  std::fill(hllByteArr, hllByteArr + numBytes, 0);
}

Hll8Array::Hll8Array(const Hll8Array& that) :
  HllArray(that)
{
  // can determine hllByteArr size in parent class, no need to allocate here
}

Hll8Array::~Hll8Array() {
  // hllByteArr deleted in parent
}

Hll8Array* Hll8Array::copy() const {
  return new Hll8Array(*this);
}

std::unique_ptr<PairIterator> Hll8Array::getIterator() const {
  PairIterator* itr = new Hll8Iterator(*this, 1 << lgConfigK);
  return std::unique_ptr<PairIterator>(itr);
}

int Hll8Array::getSlot(const int slotNo) const {
  return (int) hllByteArr[slotNo] & HllUtil::VAL_MASK_6;
}

void Hll8Array::putSlot(const int slotNo, const int value) {
  hllByteArr[slotNo] = value & HllUtil::VAL_MASK_6;
}

int Hll8Array::getHllByteArrBytes() const {
  return hll8ArrBytes(lgConfigK);
}

}

