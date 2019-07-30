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

#ifndef _INTARRAYPAIRITERATOR_INTERNAL_HPP_
#define _INTARRAYPAIRITERATOR_INTERNAL_HPP_

#include <iomanip>
#include <sstream>

#include "HllUtil.hpp"
#include "IntArrayPairIterator.hpp"

namespace datasketches {

template<typename A>
IntArrayPairIterator<A>::IntArrayPairIterator(const int* array, const int len, const int lgConfigK)
  : array(array),
    slotMask((1 << lgConfigK) - 1),
    lengthPairs(len) {
  index = -1;
  pair = -1;
}

template<typename A>
std::string IntArrayPairIterator<A>::getHeader() {
  std::ostringstream ss;
  ss << std::left
     << std::setw(10) << "Index"
     << std::setw(10) << "Key"
     << std::setw(10) << "Slot"
     << std::setw(6) << "Value";
  return ss.str();
}

template<typename A>
std::string IntArrayPairIterator<A>::getString() {
  std::ostringstream ss;
  ss << std::left
     << std::setw(10) << getIndex()
     << std::setw(10) << getKey()
     << std::setw(10) << getSlot()
     << std::setw(6) << getValue();
  return ss.str();
}

template<typename A>
int IntArrayPairIterator<A>::getIndex() {
  return index;
}

template<typename A>
int IntArrayPairIterator<A>::getKey() {
  return HllUtil<A>::getLow26(pair);
}

template<typename A>
int IntArrayPairIterator<A>::getPair() {
  return pair;
}

template<typename A>
int IntArrayPairIterator<A>::getSlot() {
  return getKey() & slotMask;
}

template<typename A>
int IntArrayPairIterator<A>::getValue() {
  return HllUtil<A>::getValue(pair);
}

template<typename A>
bool IntArrayPairIterator<A>::nextAll() {
  if (++index < lengthPairs) {
    pair = array[index];
    return true;
  }
  return false;
}

template<typename A>
bool IntArrayPairIterator<A>::nextValid() {
  while (++index < lengthPairs) {
    const int p = array[index];
    if (p != HllUtil<A>::EMPTY) {
      pair = p;
      return true;
    }
  }
  return false;
}

}

#endif // _INTARRAYPAIRITERATOR_INTERNAL_HPP_
