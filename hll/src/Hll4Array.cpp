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

#include "Hll4Array.hpp"

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

namespace datasketches {

Hll4Iterator::Hll4Iterator(const Hll4Array& hllArray, const int lengthPairs)
  : HllPairIterator(lengthPairs),
    hllArray(hllArray)
{}

Hll4Iterator::~Hll4Iterator() { }

int Hll4Iterator::value() {
  const int nib = hllArray.getSlot(index);
  if (nib == HllUtil<>::AUX_TOKEN) {
    // auxHashMap cannot be null here
    return hllArray.getAuxHashMap()->mustFindValueFor(index);
  } else {
    return nib + hllArray.getCurMin();
  }
}

Hll4Array::Hll4Array(const int lgConfigK) :
    HllArray(lgConfigK, TgtHllType::HLL_4) {
  const int numBytes = hll4ArrBytes(lgConfigK);
  hllByteArr = new uint8_t[numBytes];
  std::fill(hllByteArr, hllByteArr + numBytes, 0);
  auxHashMap = nullptr;
}

Hll4Array::Hll4Array(const Hll4Array& that) :
  HllArray(that)
{
  // can determine hllByteArr size in parent class, no need to allocate here
  // but parent class doesn't handle the auxHashMap
  if (that.auxHashMap != nullptr) {
    auxHashMap = that.auxHashMap->copy();
  } else {
    auxHashMap = nullptr;
  }
}

Hll4Array::~Hll4Array() {
  // hllByteArr deleted in parent
  if (auxHashMap != nullptr) {
    delete auxHashMap;
  }
}

Hll4Array* Hll4Array::copy() const {
  return new Hll4Array(*this);
}

std::unique_ptr<PairIterator> Hll4Array::getIterator() const {
  PairIterator* itr = new Hll4Iterator(*this, 1 << lgConfigK);
  return std::unique_ptr<PairIterator>(itr);
}

std::unique_ptr<PairIterator> Hll4Array::getAuxIterator() const {
  if (auxHashMap != nullptr) {
    return auxHashMap->getIterator();
  }
  return nullptr;
}

int Hll4Array::getUpdatableSerializationBytes() const {
  AuxHashMap* auxHashMap = getAuxHashMap();
  int auxBytes;
  if (auxHashMap == nullptr) {
    auxBytes = 4 << HllUtil<>::LG_AUX_ARR_INTS[lgConfigK];
  } else {
    auxBytes = 4 << auxHashMap->getLgAuxArrInts();
  }
  return HllUtil<>::HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
}

int Hll4Array::getHllByteArrBytes() const {
  return hll4ArrBytes(lgConfigK);
}

AuxHashMap* Hll4Array::getAuxHashMap() const {
  return auxHashMap;
}

void Hll4Array::putAuxHashMap(AuxHashMap* auxHashMap) {
  this->auxHashMap = auxHashMap;
}

int Hll4Array::getSlot(const int slotNo) const {
  int theByte = hllByteArr[slotNo >> 1];
  if ((slotNo & 1) > 0) { // odd?
    theByte >>= 4;
  }
  return theByte & HllUtil<>::loNibbleMask;
}

HllSketchImpl* Hll4Array::couponUpdate(const int coupon) {
  const int newValue = HllUtil<>::getValue(coupon);
  if (newValue <= 0) {
    throw std::logic_error("newValue must be a posittive integer. Found: " + std::to_string(newValue));
  }

  if (newValue <= curMin) {
    return this; // quick rejection, but only works for large N
  }

  const int configKmask = (1 << lgConfigK) - 1;
  const int slotNo = HllUtil<>::getLow26(coupon) & configKmask;
  internalHll4Update(slotNo, newValue);
  return this;
}

void Hll4Array::putSlot(const int slotNo, const int newValue) {
  const int byteno = slotNo >> 1;
  const int oldValue = hllByteArr[byteno];
  if ((slotNo & 1) == 0) { // set low nibble
    hllByteArr[byteno]
      = (uint8_t) ((oldValue & HllUtil<>::hiNibbleMask) | (newValue & HllUtil<>::loNibbleMask));
  } else { // set high nibble
    hllByteArr[byteno]
      = (uint8_t) ((oldValue & HllUtil<>::loNibbleMask) | ((newValue << 4) & HllUtil<>::hiNibbleMask));
  }
}

//In C: two-registers.c Line 836 in "hhb_abstract_set_slot_if_new_value_bigger" non-sparse
void Hll4Array::internalHll4Update(const int slotNo, const int newVal) {
  if ((slotNo < 0) || (slotNo >= (1 << lgConfigK))) {
    throw std::logic_error("slotNo must be between 0 and 1<<lgConfigK. Found: " + std::to_string(slotNo));
  }
  if (newVal <= 0) {
    throw std::logic_error("newVal must be a posittive integer. Found: " + std::to_string(newVal));
  }

  const int rawStoredOldValue = getSlot(slotNo); // could be a 0
  // this is provably a LB:
  const int lbOnOldValue = rawStoredOldValue + curMin; // lower bound, could be 0

  if (newVal > lbOnOldValue) { // 842
    // Note: if an AUX_TOKEN exists, then auxHashMap must alraedy exist
    // 846: rawStoredOldValue == AUX_TOKEN
    const int actualOldValue = (rawStoredOldValue < HllUtil<>::AUX_TOKEN)
       ? (lbOnOldValue) : (auxHashMap->mustFindValueFor(slotNo));

    if (newVal > actualOldValue) { // 848: actualOldValue could still be 0; newValue > 0
      // we know that hte array will change, but we haven't actually updated yet
      hipAndKxQIncrementalUpdate(*this, actualOldValue, newVal);

      if (newVal < curMin) {
        throw std::logic_error("newVal cannot be less than curMin at this point");
      }

      // newVal >= curMin

      const int shiftedNewValue = newVal - curMin; // 874
      // redundant since we know newVal >= curMin,
      // and lgConfigK bounds do not allow overflowing an int
      //assert(shiftedNewValue >= 0);

      if (rawStoredOldValue == HllUtil<>::AUX_TOKEN) { // 879
        // Given that we have an AUX_TOKEN, tehre are 4 cases for how to
        // actually modify the data structure

        if (shiftedNewValue >= HllUtil<>::AUX_TOKEN) { // case 1: 881
          // the byte array already contains aux token
          // This is the case where old and new values are both exceptions.
          // The 4-bit array already is AUX_TOKEN, only need to update auxHashMap
          auxHashMap->mustReplace(slotNo, newVal);
        }
        else { // case 2: 885
          // This is the hypothetical case where the old value is an exception and the new one is not,
          // which is impossible given that curMin has not changed here and newVal > oldValue
          throw std::runtime_error("Impossible case");
        }
      }
      else { // rawStoredOldValue != AUX_TOKEN
        if (shiftedNewValue >= HllUtil<>::AUX_TOKEN) { // case 3: 892
          // This is the case where the old value is not an exception and the new value is.
          // The AUX_TOKEN must be stored in the 4-bit array and the new value
          // added to the exception table
          putSlot(slotNo, HllUtil<>::AUX_TOKEN);
          if (auxHashMap == nullptr) {
            auxHashMap = new AuxHashMap(HllUtil<>::LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
          }
          auxHashMap->mustAdd(slotNo, newVal);
        }
        else { // case 4: 897
          // This is the case where neither the old value nor the new value is an exception.
          // We just overwrite the 4-bit array with the shifted new value.
          putSlot(slotNo, shiftedNewValue);
        }
      }

      // we just increased a pair value, so it might be time to change curMin
      if (actualOldValue == curMin) { // 908
        if (numAtCurMin < 1) {
          throw std::logic_error("Invalid state with < 1 entry at curMin");
        }
        decNumAtCurMin();
        while (numAtCurMin == 0) {
          shiftToBiggerCurMin(); // increases curMin by 1, builds a new aux table
          // shifts values in 4-bit table and recounts curMin
        }
      }
    } // end newVal <= actualOldValue
  } // end newValue <= lbOnOldValue -> return, no need to update array
}

// This scheme only works with two double registers (2 kxq values).
//   HipAccum, kxq0 and kxq1 remain untouched.
//   This changes curMin, numAtCurMin, hllByteArr and auxMap.
// Entering this routine assumes that all slots have valid values > 0 and <= 15.
// An AuxHashMap must exist if any values in the current hllByteArray are already 15.
// In C: again-two-registers.c Lines 710 "hhb_shift_to_bigger_curmin"
void Hll4Array::shiftToBiggerCurMin() {
  const int newCurMin = curMin + 1;
  const int configK = 1 << lgConfigK;
  const int configKmask = configK - 1;

  int numAtNewCurMin = 0;
  int numAuxTokens = 0;

  // Walk through the slots of 4-bit array decrementing stored values by one unless it
  // equals AUX_TOKEN, where it is left alone but counted to be checked later.
  // If oldStoredValue is 0 it is an error.
  // If the decremented value is 0, we increment numAtNewCurMin.
  // Because getNibble is masked to 4 bits oldStoredValue can never be > 15 or negative
  for (int i = 0; i < configK; i++) { //724
    int oldStoredValue = getSlot(i);
    if (oldStoredValue == 0) {
      throw std::runtime_error("Array slots cannot be 0 at this point.");
    }
    if (oldStoredValue < HllUtil<>::AUX_TOKEN) {
      putSlot(i, --oldStoredValue);
      if (oldStoredValue == 0) { numAtNewCurMin++; }
    } else { //oldStoredValue == AUX_TOKEN
      numAuxTokens++;
      if (auxHashMap == nullptr) {
        throw std::logic_error("auxHashMap cannot be null at this point");
      }
    }
  }

  // If old AuxHashMap exists, walk through it updating some slots and build a new AuxHashMap
  // if needed.
  AuxHashMap* newAuxMap = nullptr;
  if (auxHashMap != nullptr) {
    int slotNum;
    int oldActualVal;
    int newShiftedVal;

    std::unique_ptr<PairIterator> itr = auxHashMap->getIterator();
    while (itr->nextValid()) {
      slotNum = itr->getKey() & configKmask;
      oldActualVal = itr->getValue();
      newShiftedVal = oldActualVal - newCurMin;
      if (newShiftedVal < 0) {
        throw std::logic_error("oldActualVal < newCurMin when incrementing curMin");
      }

      if (getSlot(slotNum) != HllUtil<>::AUX_TOKEN) {
        throw std::logic_error("getSlot(slotNum) != AUX_TOKEN for item in auxiliary hash map");
      }
      // Array slot != AUX_TOKEN at getSlot(slotNum);
      if (newShiftedVal < HllUtil<>::AUX_TOKEN) { // 756
        if (newShiftedVal != 14) {
          throw std::logic_error("newShiftedVal != 14 for item in old auxHashMap despite curMin increment");
        }
        // The former exception value isn't one anymore, so it stays out of new AuxHashMap.
        // Correct the AUX_TOKEN value in the HLL array to the newShiftedVal (14).
        putSlot(slotNum, newShiftedVal);
        numAuxTokens--;
      }
      else { //newShiftedVal >= AUX_TOKEN
        // the former exception remains an exception, so must be added to the newAuxMap
        if (newAuxMap == nullptr) {
          newAuxMap = new AuxHashMap(HllUtil<>::LG_AUX_ARR_INTS[lgConfigK], lgConfigK);
        }
        newAuxMap->mustAdd(slotNum, oldActualVal);
      }
    } //end scan of oldAuxMap
  } //end if (auxHashMap != null)
  else { // oldAuxMap == null
    if (numAuxTokens != 0) {
      throw std::logic_error("No auxiliary hash map, but numAuxTokens != 0");
    }
  }

  if (newAuxMap != nullptr) {
    if (newAuxMap->getAuxCount() != numAuxTokens) {
      throw std::runtime_error("Inconsistent counts: auxCount: " + std::to_string(newAuxMap->getAuxCount())
                               + ", HLL tokesn: " + std::to_string(numAuxTokens));
    }
  }

  if (auxHashMap != nullptr) {
    delete auxHashMap;
  }
  auxHashMap = newAuxMap;

  curMin = newCurMin;
  numAtCurMin = numAtNewCurMin;
}

}