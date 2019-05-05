/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
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
Hll8Array<A>::Hll8Array(const int lgConfigK) :
    HllArray<A>(lgConfigK, TgtHllType::HLL_8) {
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
  Hll8Array<A>* hll = hll8Alloc().allocate(1);
  hll8Alloc().construct(hll, *this);  
  return hll;
}

template<typename A>
PairIterator_with_deleter<A> Hll8Array<A>::getIterator() const {
  typedef typename std::allocator_traits<A>::template rebind_alloc<Hll8Iterator<A>> itrAlloc;
  Hll8Iterator<A>* itr = itrAlloc().allocate(1);
  itrAlloc().construct(itr, *this, 1 << this->lgConfigK);
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