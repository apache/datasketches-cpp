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

#ifndef _HLL8ARRAY_INTERNAL_HPP_
#define _HLL8ARRAY_INTERNAL_HPP_

#include "Hll8Array.hpp"

#include <cstring>

namespace datasketches {

template<typename A>
Hll8Iterator<A>::Hll8Iterator(const Hll8Array<A>& hllArray, const int lengthPairs)
  : HllPairIterator<A>(lengthPairs),
    hllArray(hllArray)
{}

template<typename A>
Hll8Iterator<A>::~Hll8Iterator() { }

template<typename A>
int Hll8Iterator<A>::value() {
  return hllArray.hllByteArr[this->index] & HllUtil<A>::VAL_MASK_6;
}

template<typename A>
Hll8Array<A>::Hll8Array(const int lgConfigK, const bool startFullSize) :
    HllArray<A>(lgConfigK, TgtHllType::HLL_8, startFullSize) {
  const int numBytes = this->hll8ArrBytes(lgConfigK);
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint8_t> uint8Alloc;
  this->hllByteArr = uint8Alloc().allocate(numBytes);
  std::fill(this->hllByteArr, this->hllByteArr + numBytes, 0);
}

template<typename A>
Hll8Array<A>::Hll8Array(const Hll8Array<A>& that) :
  HllArray<A>(that)
{
  // can determine hllByteArr size in parent class, no need to allocate here
}

template<typename A>
Hll8Array<A>::~Hll8Array() {
  // hllByteArr deleted in parent
}

template<typename A>
std::function<void(HllSketchImpl<A>*)> Hll8Array<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    typedef typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>> hll8Alloc;
    Hll8Array<A>* hll = static_cast<Hll8Array<A>*>(ptr);
    hll->~Hll8Array();
    hll8Alloc().deallocate(hll, 1);
  };
}

template<typename A>
Hll8Array<A>* Hll8Array<A>::copy() const {
  typedef typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>> hll8Alloc;
  return new (hll8Alloc().allocate(1)) Hll8Array<A>(*this);
}

template<typename A>
PairIterator_with_deleter<A> Hll8Array<A>::getIterator() const {
  typedef typename std::allocator_traits<A>::template rebind_alloc<Hll8Iterator<A>> itrAlloc;
  Hll8Iterator<A>* itr = new (itrAlloc().allocate(1)) Hll8Iterator<A>(*this, 1 << this->lgConfigK);
  return PairIterator_with_deleter<A>(
    itr,
    [](PairIterator<A>* ptr) {
      Hll8Iterator<A>* hll = static_cast<Hll8Iterator<A>*>(ptr);
      hll->~Hll8Iterator();
      itrAlloc().deallocate(hll, 1);
    }
  );
}

template<typename A>
int Hll8Array<A>::getSlot(const int slotNo) const {
  return (int) this->hllByteArr[slotNo] & HllUtil<A>::VAL_MASK_6;
}

template<typename A>
void Hll8Array<A>::putSlot(const int slotNo, const int value) {
  this->hllByteArr[slotNo] = value & HllUtil<A>::VAL_MASK_6;
}

template<typename A>
int Hll8Array<A>::getHllByteArrBytes() const {
  return this->hll8ArrBytes(this->lgConfigK);
}

template<typename A>
HllSketchImpl<A>* Hll8Array<A>::couponUpdate(const int coupon) { // used by HLL_8 and HLL_6
  const int configKmask = (1 << this->lgConfigK) - 1;
  const int slotNo = HllUtil<A>::getLow26(coupon) & configKmask;
  const int newVal = HllUtil<A>::getValue(coupon);
  if (newVal <= 0) {
    throw std::logic_error("newVal must be a positive integer: " + std::to_string(newVal));
  }

  const int curVal = getSlot(slotNo);
  if (newVal > curVal) {
    putSlot(slotNo, newVal);
    this->hipAndKxQIncrementalUpdate(*this, curVal, newVal);
    if (curVal == 0) {
      this->decNumAtCurMin(); // interpret numAtCurMin as num zeros
      if (this->getNumAtCurMin() < 0) { 
        throw std::logic_error("getNumAtCurMin() must return a nonnegative integer: " + std::to_string(this->getNumAtCurMin()));
      }
    }
  }
  return this;
}

}

#endif // _HLL8ARRAY_INTERNAL_HPP_
