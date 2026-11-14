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

#ifndef _HLL4ARRAY_INTERNAL_HPP_
#define _HLL4ARRAY_INTERNAL_HPP_

#include "Hll4Array.hpp"

#include <algorithm>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

namespace datasketches {

template<typename A>
Hll4Array<A>::Hll4Array(uint8_t lgConfigK, const A& allocator):
HllArray<A>(lgConfigK, target_hll_type::HLL_4, allocator),
auxHashMap_(nullptr)
{
  const uint32_t numBytes = this->hll4ArrBytes(lgConfigK);
  this->hllByteArr_.resize(numBytes, 0);
}

template<typename A>
Hll4Array<A>::Hll4Array(const Hll4Array<A>& that) :
  HllArray<A>(that)
{
  // can determine hllByteArr size in parent class, no need to allocate here
  // but parent class doesn't handle the auxHashMap
  if (that.auxHashMap_ != nullptr) {
    auxHashMap_ = that.auxHashMap_->copy();
  } else {
    auxHashMap_ = nullptr;
  }
}

template<typename A>
Hll4Array<A>::Hll4Array(const HllArray<A>& other) :
  Hll4Array<A>(other.getLgConfigK(), other.getAllocator())
{
  this->oooFlag_ = other.isOutOfOrderFlag();

  for (const auto coupon : other) { // all = false, so skip empty values
    internalHll4Update(HllUtil<A>::getLow26(coupon), HllUtil<A>::getValue(coupon));
  }
  this->hipAccum_ = other.getHipAccum();
  this->rebuild_kxq_curmin_ = false;
}

template<typename A>
Hll4Array<A>::~Hll4Array() {
  // hllByteArr deleted in parent
  if (auxHashMap_ != nullptr) {
    AuxHashMap<A>::make_deleter()(auxHashMap_);
  }
}

template<typename A>
std::function<void(HllSketchImpl<A>*)> Hll4Array<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    Hll4Array<A>* hll = static_cast<Hll4Array<A>*>(ptr);
    using Hll4Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll4Array<A>>;
    Hll4Alloc hll4Alloc(hll->getAllocator());
    hll->~Hll4Array();
    hll4Alloc.deallocate(hll, 1);
  };
}

template<typename A>
Hll4Array<A>* Hll4Array<A>::copy() const {
  return HllUtil<A>::template newObject<Hll4Array<A>>(this->getAllocator(), *this);
}

template<typename A>
uint32_t Hll4Array<A>::getUpdatableSerializationBytes() const {
  AuxHashMap<A>* auxHashMap = getAuxHashMap();
  uint32_t auxBytes;
  if (auxHashMap == nullptr) {
    auxBytes = 4 << hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_];
  } else {
    auxBytes = 4 << auxHashMap->getLgAuxArrInts();
  }
  return hll_constants::HLL_BYTE_ARR_START + getHllByteArrBytes() + auxBytes;
}

template<typename A>
uint32_t Hll4Array<A>::getHllByteArrBytes() const {
  return this->hll4ArrBytes(this->lgConfigK_);
}

template<typename A>
AuxHashMap<A>* Hll4Array<A>::getAuxHashMap() const {
  return auxHashMap_;
}

template<typename A>
void Hll4Array<A>::putAuxHashMap(AuxHashMap<A>* auxHashMap) {
  this->auxHashMap_ = auxHashMap;
}

template<typename A>
uint8_t Hll4Array<A>::getSlot(uint32_t slotNo) const {
  const uint8_t byte = this->hllByteArr_[slotNo >> 1];
  if ((slotNo & 1) > 0) { // odd?
    return byte >> 4;
  }
  return byte & hll_constants::loNibbleMask;
}

template<typename A>
uint8_t Hll4Array<A>::adjustRawValue(uint32_t slot, uint8_t value) const {
  if (value != hll_constants::AUX_TOKEN) { return value + this->curMin_; }
  return auxHashMap_->mustFindValueFor(slot);
}

template<typename A>
HllSketchImpl<A>* Hll4Array<A>::couponUpdate(uint32_t coupon) {
  internalCouponUpdate(coupon);
  return this;
}

template<typename A>
void Hll4Array<A>::internalCouponUpdate(uint32_t coupon) {
  const uint8_t newValue = HllUtil<A>::getValue(coupon);
  if (newValue <= this->curMin_) {
    return; // quick rejection, but only works for large N
  }
  const uint32_t configKmask = (1 << this->lgConfigK_) - 1;
  const uint32_t slotNo = HllUtil<A>::getLow26(coupon) & configKmask;
  internalHll4Update(slotNo, newValue);
}

template<typename A>
void Hll4Array<A>::putSlot(uint32_t slotNo, uint8_t newValue) {
  const uint32_t byteno = slotNo >> 1;
  const uint8_t oldValue = this->hllByteArr_[byteno];
  if ((slotNo & 1) == 0) { // set low nibble
    this->hllByteArr_[byteno]
      = ((oldValue & hll_constants::hiNibbleMask) | (newValue & hll_constants::loNibbleMask));
  } else { // set high nibble
    this->hllByteArr_[byteno]
      = ((oldValue & hll_constants::loNibbleMask) | ((newValue << 4) & hll_constants::hiNibbleMask));
  }
}

template<typename A>
void Hll4Array<A>::internalHll4Update(uint32_t slotNo, uint8_t newVal) {
  const uint8_t rawStoredOldValue = getSlot(slotNo);
  const uint8_t lbOnOldValue = rawStoredOldValue + this->curMin_;
  if (newVal <= lbOnOldValue) return;
  const uint8_t shiftedNewValue = static_cast<uint8_t>(newVal - this->curMin_);
  if (rawStoredOldValue == hll_constants::AUX_TOKEN || shiftedNewValue >= hll_constants::AUX_TOKEN) {
    internalHll4UpdateAux(slotNo, newVal, rawStoredOldValue);
    return;
  }
  if (rawStoredOldValue == 0) {
    if (this->numAtCurMin_ == 1) {
      shiftToBiggerCurMin(slotNo, newVal);
      return;
    }
    --this->numAtCurMin_;
  }
  this->hipAndKxQIncrementalUpdate(lbOnOldValue, newVal);
  putSlot(slotNo, shiftedNewValue);
}

template<typename A>
void Hll4Array<A>::internalHll4UpdateAux(uint32_t slotNo, uint8_t newVal, uint8_t rawStoredOldValue) {
  const uint8_t actualOldValue = rawStoredOldValue < hll_constants::AUX_TOKEN
      ? static_cast<uint8_t>(rawStoredOldValue + this->curMin_) : auxHashMap_->mustFindValueFor(slotNo);
  if (newVal <= actualOldValue) return;

  if (actualOldValue == this->curMin_ && this->numAtCurMin_ == 1) {
    shiftToBiggerCurMin(slotNo, newVal);
    return;
  }

  if (rawStoredOldValue == hll_constants::AUX_TOKEN) {
    auxHashMap_->mustReplace(slotNo, newVal);
  } else {
    if (auxHashMap_ == nullptr) {
      createAuxHashMap(slotNo, newVal);
    } else {
      auxHashMap_->mustAdd(slotNo, newVal);
    }
    putSlot(slotNo, hll_constants::AUX_TOKEN);
  }

  this->hipAndKxQIncrementalUpdate(actualOldValue, newVal);
  if (actualOldValue == this->curMin_) --this->numAtCurMin_;
}

template<typename A>
void Hll4Array<A>::createAuxHashMap(uint32_t slotNo, uint8_t value) {
  auto deleter = [this](AuxHashMap<A>* map) {
    using Alloc = typename std::allocator_traits<A>::template rebind_alloc<AuxHashMap<A>>;
    Alloc alloc(this->getAllocator());
    map->~AuxHashMap();
    alloc.deallocate(map, 1);
  };
  std::unique_ptr<AuxHashMap<A>, decltype(deleter)> ptr(
      AuxHashMap<A>::newAuxHashMap(hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_],
          this->lgConfigK_, this->getAllocator()), deleter);
  ptr->mustAdd(slotNo, value);
  auxHashMap_ = ptr.release();
}

template<typename A>
void Hll4Array<A>::shiftToBiggerCurMin(uint32_t slotNo, uint8_t newVal) {
  // This update removes the last register at curMin.
  // Keep the original auxiliary map until all minimum shifts succeed.
  const uint32_t configK = 1 << this->lgConfigK_;
  const uint8_t oldStoredValue = getSlot(slotNo);
  using aux_ptr = std::unique_ptr<AuxHashMap<A>, std::function<void(AuxHashMap<A>*)>>;
  aux_ptr pending(nullptr, AuxHashMap<A>::make_deleter());
  const AuxHashMap<A>* current = auxHashMap_;
  if (newVal - this->curMin_ >= hll_constants::AUX_TOKEN) {
    pending.reset(current == nullptr
        ? AuxHashMap<A>::newAuxHashMap(hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_],
            this->lgConfigK_, this->getAllocator())
        : current->copy());
    if (oldStoredValue == hll_constants::AUX_TOKEN) pending->mustReplace(slotNo, newVal);
    else pending->mustAdd(slotNo, newVal);
    current = pending.get();
  }

  putSlot(slotNo, std::min(static_cast<uint8_t>(newVal - this->curMin_), hll_constants::AUX_TOKEN));
  uint8_t curMin = this->curMin_;
  uint32_t numAtNewCurMin = 0;
  uint32_t shiftedSlots = 0;
  try {
    do {
      const uint8_t newCurMin = static_cast<uint8_t>(curMin + 1);
      numAtNewCurMin = 0;
      uint32_t numAuxTokens = 0;
      for (shiftedSlots = 0; shiftedSlots < configK; ++shiftedSlots) {
        uint8_t value = getSlot(shiftedSlots);
        if (value == 0) {
          throw std::runtime_error("Array slots cannot be 0 at this point.");
        }
        if (value < hll_constants::AUX_TOKEN) {
          putSlot(shiftedSlots, --value);
          if (value == 0) ++numAtNewCurMin;
        } else {
          ++numAuxTokens;
          if (current == nullptr) {
            throw std::logic_error("auxHashMap cannot be null at this point");
          }
        }
      }

      aux_ptr next(nullptr, AuxHashMap<A>::make_deleter());
      if (current != nullptr) {
        for (const auto coupon: *current) {
          const uint32_t slot = HllUtil<A>::getLow26(coupon) & (configK - 1);
          const uint8_t value = HllUtil<A>::getValue(coupon);
          if (value < newCurMin) {
            throw std::logic_error("oldActualVal < newCurMin when incrementing curMin");
          }
          if (getSlot(slot) != hll_constants::AUX_TOKEN) {
            throw std::logic_error("getSlot(slotNum) != AUX_TOKEN for item in auxiliary hash map");
          }
          const uint8_t shifted = static_cast<uint8_t>(value - newCurMin);
          if (shifted < hll_constants::AUX_TOKEN) {
            if (shifted != 14) {
              throw std::logic_error("newShiftedVal != 14 for item in old auxHashMap despite curMin increment");
            }
            putSlot(slot, shifted);
            --numAuxTokens;
          } else {
            if (next == nullptr) {
              next.reset(AuxHashMap<A>::newAuxHashMap(hll_constants::LG_AUX_ARR_INTS[this->lgConfigK_],
                  this->lgConfigK_, this->getAllocator()));
            }
            next->mustAdd(slot, value);
          }
        }
      }
      const uint32_t newAuxCount = next == nullptr ? 0 : next->getAuxCount();
      // Preserve the missing-map error if another shift is required.
      if ((next != nullptr || numAtNewCurMin != 0) && newAuxCount != numAuxTokens) {
        throw std::runtime_error("Inconsistent counts: auxCount: " + std::to_string(newAuxCount)
            + ", HLL tokesn: " + std::to_string(numAuxTokens));
      }
      pending = std::move(next);
      current = pending.get();
      curMin = newCurMin;
      shiftedSlots = 0;
    } while (numAtNewCurMin == 0);
  } catch (...) {
    // Undo completed shifts and the written prefix; demoted exceptions saturate back to AUX_TOKEN.
    for (uint32_t i = 0; i < configK; ++i) {
      const uint8_t value = getSlot(i);
      if (value < hll_constants::AUX_TOKEN) {
        const uint8_t restored = static_cast<uint8_t>(value + curMin - this->curMin_ + (i < shiftedSlots));
        putSlot(i, std::min(restored, hll_constants::AUX_TOKEN));
      }
    }
    putSlot(slotNo, oldStoredValue);
    throw;
  }

  this->hipAndKxQIncrementalUpdate(this->curMin_, newVal);
  if (auxHashMap_ != nullptr) AuxHashMap<A>::make_deleter()(auxHashMap_);
  auxHashMap_ = pending.release();
  this->curMin_ = curMin;
  this->numAtCurMin_ = numAtNewCurMin;
}

template<typename A>
typename HllArray<A>::const_iterator Hll4Array<A>::begin(bool all) const {
  return typename HllArray<A>::const_iterator(this->hllByteArr_.data(), 1 << this->lgConfigK_, 0, this->tgtHllType_,
      auxHashMap_, this->curMin_, all);
}

template<typename A>
typename HllArray<A>::const_iterator Hll4Array<A>::end() const {
  return typename HllArray<A>::const_iterator(this->hllByteArr_.data(), 1 << this->lgConfigK_, 1 << this->lgConfigK_,
      this->tgtHllType_, auxHashMap_, this->curMin_, false);
}

}

#endif // _HLL4ARRAY_INTERNAL_HPP_
