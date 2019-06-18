/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLPAIRITERATOR_INTERNAL_HPP_
#define _HLLPAIRITERATOR_INTERNAL_HPP_

#include <sstream>
#include <iomanip>

#include "HllUtil.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

template<typename A>
HllPairIterator<A>::HllPairIterator(const int lengthPairs)
  : lengthPairs(lengthPairs),
    index(-1),
    val(-1)
{ }

template<typename A>
std::string HllPairIterator<A>::getHeader() {
  std::ostringstream ss;
  ss << std::setw(10) << "Slot" << std::setw(6) << "Value";
  return ss.str();
}

template<typename A>
int HllPairIterator<A>::getIndex() {
  return index;
}

template<typename A>
int HllPairIterator<A>::getKey() {
  return index;
}

template<typename A>
int HllPairIterator<A>::getSlot() {
  return index;
}

template<typename A>
int HllPairIterator<A>::getPair() {
  return HllUtil<A>::pair(index, val);
}

template<typename A>
int HllPairIterator<A>::getValue() {
  return val;
}

template<typename A>
std::string HllPairIterator<A>::getString() {
  std::ostringstream ss;
  ss << std::setw(10) << getSlot() << std::setw(6) << getValue();
  return ss.str();
}

template<typename A>
bool HllPairIterator<A>::nextAll() {
  if (++index < lengthPairs) {
    val = value();
    return true;
  }
  return false;
}

template<typename A>
bool HllPairIterator<A>::nextValid() {
  while (++index < lengthPairs) {
    val = value();
    if (val != HllUtil<A>::EMPTY) {
      return true;
    }
  }
  return false;
}

}

#endif // _HLLPAIRITERATOR_INTERNAL_HPP_
