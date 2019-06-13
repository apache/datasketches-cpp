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

#include <sstream>
#include <iomanip>

#include "HllUtil.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

HllPairIterator::HllPairIterator(const int lengthPairs)
  : lengthPairs(lengthPairs),
    index(-1),
    val(-1)
{ }

std::string HllPairIterator::getHeader() {
  std::ostringstream ss;
  ss << std::setw(10) << "Slot" << std::setw(6) << "Value";
  return ss.str();
}

int HllPairIterator::getIndex() {
  return index;
}

int HllPairIterator::getKey() {
  return index;
}

int HllPairIterator::getSlot() {
  return index;
}

int HllPairIterator::getPair() {
  return HllUtil::pair(index, val);
}

int HllPairIterator::getValue() {
  return val;
}

std::string HllPairIterator::getString() {
  std::ostringstream ss;
  ss << std::setw(10) << getSlot() << std::setw(6) << getValue();
  return ss.str();
}

bool HllPairIterator::nextAll() {
  if (++index < lengthPairs) {
    val = value();
    return true;
  }
  return false;
}

bool HllPairIterator::nextValid() {
  while (++index < lengthPairs) {
    val = value();
    if (val != HllUtil::EMPTY) {
      return true;
    }
  }
  return false;
}

}


