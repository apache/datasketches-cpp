/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <iomanip>
#include <sstream>

#include "HllUtil.hpp"
#include "IntArrayPairIterator.hpp"

namespace datasketches {

IntArrayPairIterator::IntArrayPairIterator(const int* array, const int len, const int lgConfigK)
  : array(array),
    slotMask((1 << lgConfigK) - 1),
    lengthPairs(len) {
  index = -1;
  pair = -1;
}

IntArrayPairIterator::~IntArrayPairIterator() {
  // we don't own array so nothing to do
}

std::string IntArrayPairIterator::getHeader() {
  std::ostringstream ss;
  ss << std::left
     << std::setw(10) << "Index"
     << std::setw(10) << "Key"
     << std::setw(10) << "Slot"
     << std::setw(6) << "Value";
  return ss.str();
}

std::string IntArrayPairIterator::getString() {
  std::ostringstream ss;
  ss << std::left
     << std::setw(10) << getIndex()
     << std::setw(10) << getKey()
     << std::setw(10) << getSlot()
     << std::setw(6) << getValue();
  return ss.str();
}


int IntArrayPairIterator::getIndex() {
  return index;
}

int IntArrayPairIterator::getKey() {
  return HllUtil<>::getLow26(pair);
}

int IntArrayPairIterator::getPair() {
  return pair;
}

int IntArrayPairIterator::getSlot() {
  return getKey() & slotMask;
}

int IntArrayPairIterator::getValue() {
  return HllUtil<>::getValue(pair);
}

bool IntArrayPairIterator::nextAll() {
  if (++index < lengthPairs) {
    pair = array[index];
    return true;
  }
  return false;
}

bool IntArrayPairIterator::nextValid() {
  while (++index < lengthPairs) {
    const int p = array[index];
    if (p != HllUtil<>::EMPTY) {
      pair = p;
      return true;
    }
  }
  return false;
}

}
