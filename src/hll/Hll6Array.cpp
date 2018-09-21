/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cstring>

#include "Hll6Array.hpp"

namespace datasketches {

Hll6Iterator::Hll6Iterator(const Hll6Array& hllArray, const int lengthPairs)
  : HllPairIterator(lengthPairs),
    hllArray(hllArray),
    bitOffset(-6)
{}

Hll6Iterator::~Hll6Iterator() { }

int Hll6Iterator::value() {
  bitOffset += 6;
  const int shift = bitOffset & 0x7;
  const int byteIdx = bitOffset >> 3;
  const uint16_t twoByteVal = (hllArray.hllByteArr[byteIdx + 1] << 8)
                              | hllArray.hllByteArr[byteIdx];
  return (uint8_t) (twoByteVal >> shift) & 0x3F;
}

Hll6Array::Hll6Array(const int lgConfigK) :
    HllArray(lgConfigK, TgtHllType::HLL_6) {
  const int numBytes = hll6ArrBytes(lgConfigK);
  hllByteArr = new uint8_t[numBytes];
  std::fill(hllByteArr, hllByteArr + numBytes, 0);
}

Hll6Array::Hll6Array(const Hll6Array& that) :
  HllArray(that)
{
  // can determine hllByteArr size in parent class, no need to allocate here
}

Hll6Array::~Hll6Array() {
  // hllByteArr deleted in parent
}

Hll6Array* Hll6Array::copy() const {
  return new Hll6Array(*this);
}

std::unique_ptr<PairIterator> Hll6Array::getIterator() const {
  PairIterator* itr = new Hll6Iterator(*this, 1 << lgConfigK);
  return std::unique_ptr<PairIterator>(itr);
}

int Hll6Array::getSlot(const int slotNo) const {
  const int startBit = slotNo * 6;
  const int shift = startBit & 0x7;
  const int byteIdx = startBit >> 3;  
  const uint16_t twoByteVal = (hllByteArr[byteIdx + 1] << 8) | hllByteArr[byteIdx];
  return (uint8_t) (twoByteVal >> shift) & 0x3F;
}

void Hll6Array::putSlot(const int slotNo, const int value) {
  const int startBit = slotNo * 6;
  const int shift = startBit & 0x7;
  const int byteIdx = startBit >> 3;
  const uint16_t valShifted = (value & 0x3F) << shift;
  uint16_t curMasked = (hllByteArr[byteIdx + 1] << 8) | hllByteArr[byteIdx];
  curMasked &= (~(HllUtil::VAL_MASK_6 << shift));
  uint16_t insert = curMasked | valShifted;
  hllByteArr[byteIdx]     = insert & 0xFF;
  hllByteArr[byteIdx + 1] = (insert & 0xFF00) >> 8;
}

int Hll6Array::getHllByteArrBytes() const {
  return hll6ArrBytes(lgConfigK);
}

}

