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
  curMasked &= (~(HllUtil<>::VAL_MASK_6 << shift));
  uint16_t insert = curMasked | valShifted;
  hllByteArr[byteIdx]     = insert & 0xFF;
  hllByteArr[byteIdx + 1] = (insert & 0xFF00) >> 8;
}

int Hll6Array::getHllByteArrBytes() const {
  return hll6ArrBytes(lgConfigK);
}

HllSketchImpl* Hll6Array::couponUpdate(const int coupon) {
  const int configKmask = (1 << getLgConfigK()) - 1;
  const int slotNo = HllUtil<>::getLow26(coupon) & configKmask;
  const int newVal = HllUtil<>::getValue(coupon);
  if (newVal <= 0) {
    throw std::logic_error("newVal must be a positive integer: " + std::to_string(newVal));
  }

  const int curVal = getSlot(slotNo);
  if (newVal > curVal) {
    putSlot(slotNo, newVal);
    hipAndKxQIncrementalUpdate(*this, curVal, newVal);
    if (curVal == 0) {
      decNumAtCurMin(); // interpret numAtCurMin as num zeros
      if (getNumAtCurMin() < 0) { 
        throw std::logic_error("getNumAtCurMin() must return a nonnegative integer: " + std::to_string(getNumAtCurMin()));
      }
    }
  }
  return this;
}


}

