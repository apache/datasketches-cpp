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
